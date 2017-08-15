// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package topom

import (
	"github.com/CodisLabs/codis/pkg/models"
	"github.com/CodisLabs/codis/pkg/utils/errors"
	"github.com/CodisLabs/codis/pkg/utils/log"
	"github.com/CodisLabs/codis/pkg/utils/sync2"
)

func (s *Topom) CreateNamespace(ns models.Namespace) error {
	if ns.Id == "" || ns.Password == "" {
		return errors.Errorf("namespace-[%s] create failed %s", ns.Id, ns.Encode())
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return err
	}
	if ctx.namespace[ns.Id] != nil {
		return errors.Errorf("namespace-[%s] already exists", ns.Id)
	}
	defer s.dirtyNamespaceCache(ns.Id)
	return s.storeNamespace(&ns)
}
func (s *Topom) ResyncNamespace(nid string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return err
	}

	n, err := ctx.getNamespace(nid)
	if err != nil {
		return err
	}

	if err := s.resyncNamespace(ctx, n); err != nil {
		log.Warnf("namespace-[%d] resync-namespace failed", nid)
		return err
	}
	defer s.dirtyNamespaceCache(nid)

	return nil
}
func (s *Topom) ResyncNamespaceAll() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return err
	}
	log.Infof("resync all namespace %v", ctx.namespace)

	for _, n := range ctx.namespace {
		if err := s.resyncNamespace(ctx, n); err != nil {
			log.Warnf("namespace-[%d] resync-namespace failed", n.Id)
			return err
		}
		defer s.dirtyNamespaceCache(n.Id)
	}
	return nil
}

func (s *Topom) resyncNamespace(ctx *context, ns *models.Namespace) error {
	if ns == nil {
		return nil
	}
	var fut sync2.Future
	for _, p := range ctx.proxy {
		fut.Add()
		go func(p *models.Proxy) {
			err := s.newProxyClient(p).FillNamespaces(ns)
			if err != nil {
				log.ErrorErrorf(err, "proxy-[%s] resync namespace failed", p.Token)
			}
			fut.Done(p.Token, err)
		}(p)
	}
	for t, v := range fut.Wait() {
		switch err := v.(type) {
		case error:
			if err != nil {
				return errors.Errorf("proxy-[%s] resync namespace failed", t)
			}
		}
	}
	return nil
}
