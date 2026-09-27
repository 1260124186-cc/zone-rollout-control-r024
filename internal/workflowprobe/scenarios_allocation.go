package workflowprobe

import (
	"net/http"

	"solo-0014-zone-rollout-control/internal/domain"
)

func runNodeLeaseAllocation() error {
	harness, err := newHarness()
	if err != nil {
		return err
	}
	defer harness.close()

	if _, err := enrollNode(harness, "resolver-east-01", "east"); err != nil {
		return err
	}
	if _, err := enrollNode(harness, "resolver-east-02", "east"); err != nil {
		return err
	}
	plan, _, err := createActivePlan(harness, "lease.example", 1001, 2, 0.5, 100)
	if err != nil {
		return err
	}
	first, err := allocate(harness, plan.ID, "lease-request-0001", "east", 2)
	if err != nil {
		return err
	}
	if len(first.LeaseIDs) != 2 {
		return ensure(false, "first lease group must contain two leases")
	}
	if err := expectLeasedNodeViews(harness, plan, first, "east"); err != nil {
		return err
	}
	replay, err := allocate(harness, plan.ID, "lease-request-0001", "east", 2)
	if err != nil {
		return err
	}
	if replay.ID != first.ID {
		return ensure(false, "lease request replay returned another group")
	}
	_, err = allocate(harness, plan.ID, "lease-request-0002", "east", 2)
	if err := expectFailure(err, http.StatusConflict, string(domain.CodeNodeUnavailable)); err != nil {
		return err
	}
	var released domain.NodeLease
	if err := harness.call(http.MethodPatch, "/v1/leases/"+first.LeaseIDs[0]+"/release", nil, &released); err != nil {
		return err
	}
	if released.State != domain.LeaseReleased {
		return ensure(false, "lease release did not update state")
	}
	page, err := listNodes(harness, "region=east")
	if err != nil {
		return err
	}
	if len(page.Items) != 2 {
		return ensure(false, "region filter must return both nodes")
	}
	states := make(map[domain.NodeState]int)
	for _, item := range page.Items {
		states[item.State]++
		switch item.State {
		case domain.NodeReady:
			if item.ActiveLease != nil {
				return ensure(false, "ready node must not show an active lease")
			}
		case domain.NodeLeased:
			if item.ActiveLease == nil {
				return ensure(false, "leased node is missing its active lease")
			}
		}
	}
	if states[domain.NodeReady] != 1 || states[domain.NodeLeased] != 1 {
		return ensure(false, "release must return exactly one node to ready")
	}
	return nil
}

func expectLeasedNodeViews(
	harness *harness,
	plan domain.RolloutPlan,
	group domain.LeaseGroup,
	region string,
) error {
	leaseIDs := make(map[string]struct{}, len(group.LeaseIDs))
	for _, id := range group.LeaseIDs {
		leaseIDs[id] = struct{}{}
	}
	page, err := listNodes(harness, "state=leased&region="+region)
	if err != nil {
		return err
	}
	if len(page.Items) != len(group.LeaseIDs) {
		return ensure(false, "leased filter must return every leased node")
	}
	now := harness.clock.Now()
	for _, item := range page.Items {
		if item.ActiveLease == nil {
			return ensure(false, "leased node is missing its active lease summary")
		}
		if _, ok := leaseIDs[item.ActiveLease.LeaseID]; !ok {
			return ensure(false, "active lease is not part of the allocation group")
		}
		if item.ActiveLease.PlanID != plan.ID {
			return ensure(false, "active lease points at the wrong plan")
		}
		if !item.ActiveLease.ExpiresAt.After(now) {
			return ensure(false, "active lease expiry must be in the future")
		}
	}
	readyPage, err := listNodes(harness, "state=ready&region="+region)
	if err != nil {
		return err
	}
	if len(readyPage.Items) != 0 {
		return ensure(false, "ready filter must not return leased nodes")
	}
	firstPage, err := listNodes(harness, "region="+region+"&limit=1")
	if err != nil {
		return err
	}
	if len(firstPage.Items) != 1 || firstPage.Meta.NextCursor == "" {
		return ensure(false, "paged node listing must return one item and a cursor")
	}
	secondPage, err := listNodes(harness, "region="+region+"&limit=1&cursor="+firstPage.Meta.NextCursor)
	if err != nil {
		return err
	}
	if len(secondPage.Items) != 1 || secondPage.Items[0].ID == firstPage.Items[0].ID {
		return ensure(false, "cursor page must return the remaining node")
	}
	if firstPage.Items[0].ActiveLease == nil || secondPage.Items[0].ActiveLease == nil {
		return ensure(false, "paged nodes must keep their active lease summaries")
	}
	return nil
}
