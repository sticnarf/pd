// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package schedule

import (
	"sync"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/pingcap/pd/v4/server/core"
	"github.com/pingcap/pd/v4/server/schedule/filter"
	"github.com/pingcap/pd/v4/server/schedule/operator"
	"github.com/pingcap/pd/v4/server/schedule/opt"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

const regionScatterName = "region-scatter"

type selectedStores struct {
	mu       sync.Mutex
	ordinary map[uint64]struct{}
	special  map[string]map[uint64]struct{}
}

func newSelectedStores() *selectedStores {
	return &selectedStores{
		ordinary: make(map[uint64]struct{}),
		special:  make(map[string]map[uint64]struct{}),
	}
}

func (s *selectedStores) putOrdinary(id uint64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.ordinary[id]; ok {
		return false
	}
	s.ordinary[id] = struct{}{}
	return true
}

func (s *selectedStores) putSpecial(id uint64, engine string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	stores, ok := s.special[engine]
	if !ok {
		stores = make(map[uint64]struct{})
		s.special[engine] = stores
	}
	if _, ok := stores[id]; ok {
		return false
	}
	stores[id] = struct{}{}
	return true
}

func (s *selectedStores) resetOrdinary() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ordinary = make(map[uint64]struct{})
}

func (s *selectedStores) resetSpecial(engine string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.special, engine)
}

func (s *selectedStores) newFilter(scope string) filter.Filter {
	s.mu.Lock()
	defer s.mu.Unlock()
	cloned := make(map[uint64]struct{})
	for id := range s.ordinary {
		cloned[id] = struct{}{}
	}
	return filter.NewExcludedFilter(scope, nil, cloned)
}

// RegionScatterer scatters regions.
type RegionScatterer struct {
	name     string
	cluster  opt.Cluster
	filters  []filter.Filter
	selected *selectedStores
}

// NewRegionScatterer creates a region scatterer.
// RegionScatter is used for the `Lightning`, it will scatter the specified regions before import data.
func NewRegionScatterer(cluster opt.Cluster) *RegionScatterer {
	return &RegionScatterer{
		name:    regionScatterName,
		cluster: cluster,
		filters: []filter.Filter{
			filter.StoreStateFilter{ActionScope: regionScatterName},
		},
		selected: newSelectedStores(),
	}
}

// Scatter relocates the region.
func (r *RegionScatterer) Scatter(region *core.RegionInfo) (*operator.Operator, error) {
	if !opt.IsRegionReplicated(r.cluster, region) {
		return nil, errors.Errorf("region %d is not fully replicated", region.GetID())
	}

	if region.GetLeader() == nil {
		return nil, errors.Errorf("region %d has no leader", region.GetID())
	}

	return r.scatterRegion(region), nil
}

func (r *RegionScatterer) scatterRegion(region *core.RegionInfo) *operator.Operator {
	stores := r.collectAvailableStores(region)
	targetPeers := make(map[uint64]*metapb.Peer)
	ordinaryPeers, specialPeers := r.groupPeersByEngine(region)

	for _, peer := range ordinaryPeers {
		if len(stores.ordinary) == 0 {
			r.selected.resetOrdinary()
			stores = r.collectAvailableStores(region)
		}

		if _, ok := stores.ordinary[peer.GetStoreId()]; ok && r.selected.putOrdinary(peer.GetStoreId()) {
			delete(stores.ordinary, peer.GetStoreId())
			targetPeers[peer.GetStoreId()] = peer
			continue
		}
		newPeer := r.selectPeerToReplace(stores.ordinary, region, peer)
		if newPeer == nil {
			targetPeers[peer.GetStoreId()] = peer
			continue
		}
		// Remove it from stores and mark it as selected.
		delete(stores.ordinary, newPeer.GetStoreId())
		r.selected.putOrdinary(newPeer.GetStoreId())
		targetPeers[newPeer.GetStoreId()] = newPeer
	}
	for engine, peer := range specialPeers {
		engineStores := stores.special[engine]
		if len(engineStores) == 0 {
			r.selected.resetSpecial(engine)
			stores = r.collectAvailableStores(region)
			engineStores = stores.special[engine]
		}

		if _, ok := engineStores[peer.GetStoreId()]; ok && r.selected.putSpecial(peer.GetStoreId(), engine) {
			delete(engineStores, peer.GetStoreId())
			targetPeers[peer.GetStoreId()] = peer
			continue
		}
		newPeer := r.selectPeerToReplace(engineStores, region, peer)
		if newPeer == nil {
			targetPeers[peer.GetStoreId()] = peer
			continue
		}
		// Remove it from stores and mark it as selected.
		delete(engineStores, newPeer.GetStoreId())
		r.selected.putSpecial(newPeer.GetStoreId(), engine)
		targetPeers[newPeer.GetStoreId()] = newPeer
	}
	op, err := operator.CreateScatterRegionOperator("scatter-region", r.cluster, region, targetPeers)
	if err != nil {
		log.Debug("fail to create scatter region operator", zap.Error(err))
		return nil
	}
	op.SetPriorityLevel(core.HighPriority)
	return op
}

func (r *RegionScatterer) selectPeerToReplace(stores map[uint64]*core.StoreInfo, region *core.RegionInfo, oldPeer *metapb.Peer) *metapb.Peer {
	// scoreGuard guarantees that the distinct score will not decrease.
	regionStores := r.cluster.GetRegionStores(region)
	storeID := oldPeer.GetStoreId()
	sourceStore := r.cluster.GetStore(storeID)
	if sourceStore == nil {
		log.Error("failed to get the store", zap.Uint64("store-id", storeID))
		return nil
	}
	var scoreGuard filter.Filter
	if r.cluster.IsPlacementRulesEnabled() {
		scoreGuard = filter.NewRuleFitFilter(r.name, r.cluster, region, oldPeer.GetStoreId())
	} else {
		scoreGuard = filter.NewDistinctScoreFilter(r.name, r.cluster.GetLocationLabels(), regionStores, sourceStore)
	}

	candidates := make([]*core.StoreInfo, 0, len(stores))
	for _, store := range stores {
		if scoreGuard.Target(r.cluster, store) {
			candidates = append(candidates, store)
		}
	}

	if len(candidates) == 0 {
		return nil
	}

	//target := candidates[rand.Intn(len(candidates))]
	target := candidates[0]
	return &metapb.Peer{
		StoreId:   target.GetID(),
		IsLearner: oldPeer.GetIsLearner(),
	}
}

func (r *RegionScatterer) groupPeersByEngine(region *core.RegionInfo) (ordinary []*metapb.Peer, special map[string]*metapb.Peer) {
	special = make(map[string]*metapb.Peer)
	specialFilter := filter.NewSpecialEngineFilter(r.name)
	for _, peer := range region.GetPeers() {
		store := r.cluster.GetStore(peer.StoreId)
		if specialFilter.Target(r.cluster, store) {
			special[store.GetLabelValue(filter.EngineKey)] = peer
		} else {
			ordinary = append(ordinary, peer)
		}
	}
	return ordinary, special
}

type availableStores struct {
	ordinary map[uint64]*core.StoreInfo
	special  map[string]map[uint64]*core.StoreInfo
}

func (r *RegionScatterer) storesWithSameEngineType(stores availableStores, store *core.StoreInfo) map[uint64]*core.StoreInfo {
	specialFilter := filter.NewSpecialEngineFilter("region-scatter")
	if specialFilter.Target(r.cluster, store) {
		return stores.special[store.GetLabelValue(filter.EngineKey)]
	} else {
		return stores.ordinary
	}
}

func (r *RegionScatterer) collectAvailableStores(region *core.RegionInfo) availableStores {
	filters := []filter.Filter{
		r.selected.newFilter(r.name),
		filter.NewExcludedFilter(r.name, nil, region.GetStoreIds()),
	}
	filters = append(filters, r.filters...)

	stores := r.cluster.GetStores()
	specialEngineFilter := filter.NewSpecialEngineFilter(r.name)
	ordinary := make(map[uint64]*core.StoreInfo)
	special := make(map[string]map[uint64]*core.StoreInfo)
	for _, store := range stores {
		if filter.Target(r.cluster, store, filters) && !store.IsBusy() {
			if specialEngineFilter.Target(r.cluster, store) {
				engineStores, ok := special[store.GetLabelValue(filter.EngineKey)]
				if !ok {
					engineStores = make(map[uint64]*core.StoreInfo)
					special[store.GetLabelValue(filter.EngineKey)] = engineStores
				}
				engineStores[store.GetID()] = store
			} else {
				ordinary[store.GetID()] = store
			}
		}
	}
	return availableStores{
		ordinary: ordinary,
		special:  special,
	}
}
