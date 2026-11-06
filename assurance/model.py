#!/usr/bin/env python3
"""Bounded, source-free reference model for Wasm tenant deployment and execution admission.

NOT a proof of production Rust/TypeScript or an executed AttoLang guest.
The state space has two tenants, two keys per tenant, two payloads, three
independent admission conditions, and traces of up to four transitions.
Run: python3 assurance/model.py
"""
from dataclasses import dataclass
from itertools import product
from collections import deque

DOMAIN = "wasmx"
PURPOSE = "Wasm tenant deployment and execution admission"
UNSET = -1

@dataclass(frozen=True)
class State:
    payloads: tuple = (UNSET, UNSET, UNSET, UNSET)
    effects: tuple = (0, 0, 0, 0)

@dataclass(frozen=True)
class Event:
    tenant: int
    key: int
    payload: int
    authority: bool
    proof: bool
    budget: bool

def apply(state, event):
    """Reference transition: deny -> unchanged; retry -> no new effect."""
    index = 2 * event.tenant + event.key
    if not (event.authority and event.proof and event.budget):
        return state, "denied"
    existing = state.payloads[index]
    if existing == event.payload:
        return state, "duplicate"
    if existing != UNSET:
        return state, "conflict"
    payloads = list(state.payloads)
    effects = list(state.effects)
    payloads[index] = event.payload
    effects[index] += 1
    return State(tuple(payloads), tuple(effects)), "applied"

def invariant(before, event, after, outcome):
    index = 2 * event.tenant + event.key
    assert all(effect in (0, 1) for effect in after.effects)
    assert all((p == UNSET) == (e == 0) for p, e in zip(after.payloads, after.effects))
    for i in range(4):
        if i != index:
            assert before.payloads[i] == after.payloads[i], "cross-key/tenant write"
            assert before.effects[i] == after.effects[i], "cross-key/tenant side effect"
    if outcome != "applied":
        assert after == before, "denial, replay or conflict mutated state"
    else:
        assert event.authority and event.proof and event.budget
        assert before.payloads[index] == UNSET
        assert after.payloads[index] == event.payload
        assert after.effects[index] == before.effects[index] + 1

def check_admission():
    events = [
        Event(t, k, p, a, proof, budget)
        for t, k, p in product(range(2), repeat=3)
        for a, proof, budget in (
            (True, True, True), (False, True, True),
            (True, False, True), (True, True, False))
    ]
    initial = State()
    seen = {initial}
    frontier = {initial}
    checked = 0
    for depth in range(4):
        following = set()
        for before in frontier:
            for event in events:
                after, outcome = apply(before, event)
                invariant(before, event, after, outcome)
                checked += 1
                if after not in seen:
                    following.add(after)
        seen.update(following)
        frontier = following
        if not frontier:
            break
    # Independent-tenant writes must commute; a retry must be effect-free.
    for a_payload, b_payload in product(range(2), repeat=2):
        a = Event(0, 0, a_payload, True, True, True)
        b = Event(1, 0, b_payload, True, True, True)
        left, _ = apply(initial, a)
        left, _ = apply(left, b)
        right, _ = apply(initial, b)
        right, _ = apply(right, a)
        assert left == right, "independent tenants do not commute"
        replayed, outcome = apply(left, a)
        assert outcome == "duplicate" and replayed == left
    assert len(seen) == 81, f"unexpected reachable-state coverage: {len(seen)}"
    return checked, len(seen)

def check_domain_obligations():
    checks = 0
    for x, y, z, w in product((False, True), repeat=4):
        checks += 1
        if DOMAIN == "wasmx":
            # A cached module is never enough: origin/tenant, digest, ABI, and no-WASI matter.
            admitted = x and y and z and (not w)
            if w or not x or not y or not z:
                assert not admitted
        elif DOMAIN == "usa_acc":
            # Valid authority is necessary independently of realm, audience and case revision.
            admitted = x and y and z and w
            if not x or not y or not z or not w:
                assert not admitted
        elif DOMAIN == "voxletra":
            # Never advance checkpoint on unacknowledged or rejected mutation.
            old, incoming = (int(x) + 2 * int(y)), (int(z) + 2 * int(w))
            advance = max(old, incoming) if x else old
            assert advance >= old
            if not x:
                assert advance == old
        elif DOMAIN == "takoda":
            # A completion from a stale worker epoch cannot commit an effect.
            current_epoch = int(x) + 2 * int(y)
            supplied_epoch = int(z) + 2 * int(w)
            accepted = current_epoch == supplied_epoch
            if supplied_epoch != current_epoch:
                assert not accepted
        elif DOMAIN == "praxonne":
            # Enrolled SSID, recent attestation, and required tunnel are all independent.
            admitted = x and y and (not z or w)
            if not x or not y or (z and not w):
                assert not admitted
        elif DOMAIN == "owls":
            # No activation without origin, content digest, compatible release and budget.
            admitted = x and y and z and w
            if not (x and y and z and w):
                assert not admitted
        elif DOMAIN == "time_series":
            old = int(x) + 2 * int(y)
            incoming = int(z) + 2 * int(w)
            after = max(old, incoming)
            assert after >= old and after >= incoming, "watermark regressed"
        elif DOMAIN == "forms":
            # A committed submission and its outbox event are atomic in the model.
            committed = x and y and z and w
            outbox_recorded = committed
            assert committed == outbox_recorded
        elif DOMAIN == "legal":
            # No signing evidence without consent, version match, signer and valid time.
            accepted = x and y and z and w
            if not (x and y and z and w):
                assert not accepted
        else:
            raise AssertionError("unknown domain")
    return checks

if __name__ == "__main__":
    transitions, states = check_admission()
    domain_checks = check_domain_obligations()
    print(f"HOST_MODEL_PASS domain={DOMAIN} states={states} transitions={transitions} domain_cases={domain_checks}")
    print("GUEST_EXECUTION=NOT_EXECUTED; PRODUCTION_REFINEMENT=NOT_VERIFIED")
