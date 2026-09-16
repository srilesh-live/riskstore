# Chaos playbook

Faults are injected **by an operator**, never by the harness. Granting a load generator the right to SIGKILL a database replica or run `tc netem` on a production-like RHEL host is a security conversation most sites will lose, and it isn't necessary: the harness is already recording everything, so all it needs from you is a timestamp.

## How to use this

1. Start the matching scenario and note the `runId`.
2. Wait for the phase named in the procedure below.
3. Run the fault command on the target host.
4. **Immediately** post a mark:

```bash
curl -XPOST http://perfbench-host:8080/api/v1/runs/RUN_ID/marks -H 'Content-Type: application/json' -d '{"kind":"replica-kill","target":"ch-replica-2","note":"SIGKILL at T+15m"}'
```

5. Reverse the fault at the time the procedure says, and post a second mark with `kind` suffixed `-recover`.
6. Let the run finish. Marks are overlaid as vertical lines on every chart in the report.

The scraper also **auto-detects** replicas going read-only or losing their Keeper session, and raises its own marks with `source: auto`. So the timeline stays correct even if you forget to post — and an *unplanned* fault during an otherwise clean run cannot pass unnoticed.

> **Correctness beats latency in every scenario here.** A fault that costs you five seconds of p99 is a finding. A fault that costs you one row is a defect. Read the reconciliation table first.

---

## C1 — Replica loss under sustained ingest

**Scenario:** `chaos-replica-kill`  ·  **Fault at:** start of `fault` phase (T+15m)  ·  **Recover at:** start of `recover` phase (T+30m)

```bash
sudo systemctl kill -s SIGKILL clickhouse-server
```

Recover with `sudo systemctl start clickhouse-server`.

**What you are really deciding.** With only two replicas, `insert_quorum=2` means a single replica outage stops **all** writes. `insert_quorum=0` keeps writing but accepts that an unreplicated block exists briefly. That is a durability-versus-availability trade-off and it deserves measured evidence, not a default. **Run this scenario twice, once with each setting, and compare.**

| Check | Pass criteria |
|---|---|
| Data loss | Zero. `recon.rowsLost = 0` and `l0-rows-survived-fault` passes |
| Write availability | With `insert_quorum=0`, writes continue; with `=2`, they stop — confirm which you got |
| Catch-up | `ReplicationAbsoluteDelay` returns under 60s before the run ends |
| Convergence | `replica-convergence:*` checks pass — both replicas hold identical row counts |

**Watch for:** a replica that restarts but never catches up. Look at `ReplicationQueueSize` — flat-and-high means the queue is stuck, not busy.

---

## C2 — Graceful restart

**Scenario:** `chaos-replica-kill`  ·  **Fault at:** T+15m

```bash
sudo systemctl restart clickhouse-server
```

Measures the clean path: drain, rejoin, and **startup time with a realistic part count**. Startup attaches every active part, so a table carrying tens of thousands of parts can take minutes to come back. Teams are routinely surprised by this during their first real incident.

| Check | Pass criteria |
|---|---|
| Startup time | Under 120s — record the actual figure, it is your restart budget |
| Data loss | Zero |

---

## C3 — Single Keeper node loss

**Scenario:** `chaos-replica-kill`  ·  **Fault at:** T+15m  ·  **Target:** any one Keeper node

```bash
sudo systemctl stop clickhouse-keeper
```

With three nodes, quorum survives one loss. This should be a non-event — the value is confirming that, and measuring the size of the blip.

| Check | Pass criteria |
|---|---|
| Write availability | Uninterrupted |
| Keeper latency | `KeeperAvgLatencyMs` spikes briefly, then returns to baseline |
| Errors | Under 0.1% |

---

## C4 — Keeper quorum loss ⚠

**Scenario:** `chaos-replica-kill`  ·  **Fault at:** T+15m  ·  **Target:** two of three Keeper nodes

```bash
sudo systemctl stop clickhouse-keeper
```

Run on **two** Keeper hosts. Quorum is lost and **every replicated table goes read-only**. Reads continue; writes stop completely.

This is the single most important chaos scenario for this architecture, because three Keeper nodes tolerate exactly one failure and a second one is not a degradation — it is a full write outage.

| Check | Pass criteria |
|---|---|
| Detection | `ReplicaIsReadOnly` goes to 1 within 60s — the harness marks this automatically |
| Failure mode | Writes fail *fast and loudly*, not by hanging. Hanging writes back-pressure the ingestion service and turn a Keeper problem into a Kafka problem |
| Recovery | All tables writable within 120s of quorum being restored |
| Data loss | Zero for acknowledged writes |

**Also verify** the ingestion service's behaviour here, separately. A retry storm against a read-only cluster can turn a recoverable outage into a longer one.

---

## C5 — Keeper leader election

**Scenario:** `chaos-replica-kill`  ·  **Fault at:** T+15m

Identify the leader first:

```bash
clickhouse-client --query "SELECT host, is_leader, server_state FROM system.zookeeper_info"
```

Then stop that node's `clickhouse-keeper`. Measures re-election time and its cost to the insert path.

| Check | Pass criteria |
|---|---|
| Re-election | Under 30s; a new node reports `is_leader = 1` |
| Insert impact | p99 spike under 10s, no errors |

---

## C6 — Network latency between replicas

**Scenario:** `chaos-replica-kill`  ·  **Fault at:** T+15m

```bash
sudo tc qdisc add dev eth0 root netem delay 50ms
```

Remove with `sudo tc qdisc del dev eth0 root netem`.

| Check | Pass criteria |
|---|---|
| Lag growth | `ReplicationAbsoluteDelay` grows but bounded, not unbounded |
| Recovery | Returns to baseline within 5 minutes of the fault being removed |
| Keeper | `KeeperCommitLag` stays bounded |

---

## C7 — Disk exhaustion

**Scenario:** `insert-baseline-rates`  ·  **Fault at:** during `steady`

```bash
sudo fallocate -l 200G /var/lib/clickhouse/ballast.tmp
```

Remove with `sudo rm /var/lib/clickhouse/ballast.tmp`. Size the ballast to reach roughly 95% used.

**Why this matters more than it looks.** A merge needs free space of the same order as the parts it is merging. A disk at 95% stops *merging* well before it stops *accepting writes* — so part count climbs, and ingestion then stops for what looks like an unrelated reason. The failure presents as `TOO_MANY_PARTS`, not as a disk error, which is why teams misdiagnose it.

| Check | Pass criteria |
|---|---|
| Merge behaviour | Merges fail with a clear disk error, logged and visible |
| Part count | `MaxPartCountForPartition` climbs — confirm the causal chain |
| Recovery | Merges resume and part count drains after space is freed |
| Data loss | Zero for acknowledged writes |

---

## C8 — CPU starvation on one replica

**Scenario:** `mixed-eod-peak`  ·  **Fault at:** during `steady`

```bash
stress-ng --cpu 60 --timeout 600s
```

| Check | Pass criteria |
|---|---|
| Degradation | Graceful; the healthy replica absorbs load |
| Errors | No query failures, only slower ones |

---

## C9 — Long mutation during peak ingest

**Scenario:** `mixed-eod-peak`  ·  **Fault at:** during `steady`

```bash
clickhouse-client --query "ALTER TABLE riskstore.l1_rates_valuation UPDATE valuation_model = 'LMM-1' WHERE business_date = today() SETTINGS mutations_sync = 0"
```

Mutations rewrite whole parts and compete with merges for the same background pool. This is the most common self-inflicted production incident in a ClickHouse risk store: a routine data fix issued at end of day.

| Check | Pass criteria |
|---|---|
| Ingest impact | Insert p99 stays inside SLO |
| Part count | Does not breach the threshold while the mutation runs |
| Mutation progress | `MutationsPending` trends to zero; if it does not, the mutation is stuck |

---

## C10 — Restart with a high part count

**Scenario:** run `breaking-point` first to accumulate parts, then restart a replica.

```bash
sudo systemctl restart clickhouse-server
```

Startup attaches every active part. Record the wall-clock time — this is your true worst-case restart budget, and it is normally far longer than the number people quote from a quiet cluster.

| Check | Pass criteria |
|---|---|
| Startup time | Recorded and compared against C2's clean-state figure |
| Recovery | Table is queryable and writable once startup completes |

---

## After the run

1. Open the HTML report. Fault marks appear as dashed vertical lines on every chart.
2. **Read the reconciliation table before the latency tables.** A fast run that lost rows is a failed run.
3. Check the fault timeline section for `source: auto` marks you did not post — those are faults you did not intend.
4. File the report against the go-live evidence pack, including the ones that failed. A chaos scenario that fails and is understood is worth more than one that passes and is not.
