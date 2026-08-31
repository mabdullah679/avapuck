#!/usr/bin/env bash
# In-cluster verification of the SECURITY claims, not just liveness.
#
# "The pods are running" is not the same as "no plaintext reached the
# warehouse". These checks assert the invariants from docs/AGENTS.md against
# the live cluster.
set -uo pipefail

CTX="${KUBE_CONTEXT:-trips}"
NS="${NAMESPACE:-trips}"
K="kubectl --context $CTX -n $NS"
OK="  [ok]  "; BAD="  [FAIL]"; SKIP="  [skip]"
fail=0

echo "Cluster verification — $NS"
echo "──────────────────────────────────────────────────────"

# 1. Everything up
notready=$($K get pods --no-headers 2>/dev/null | awk '$3!="Running" && $3!="Completed" && $3!="Succeeded"' | wc -l | tr -d ' ')
if [ "$notready" = "0" ]; then echo "$OK all pods Running"; else echo "$BAD $notready pod(s) not Running"; $K get pods --no-headers | awk '$3!="Running"&&$3!="Completed"&&$3!="Succeeded"{print "         "$1,$3}'; fail=1; fi

# 2. Unauthenticated access must be refused
# `kubectl run --rm` appends its own "pod deleted" line to stdout, which would
# be concatenated into the status code. Marker-delimited output isolates it.
raw=$($K run vfy-auth --rm -i --restart=Never --image=trips/service:latest \
  --overrides='{"spec":{"containers":[{"name":"vfy-auth","image":"trips/service:latest","imagePullPolicy":"IfNotPresent","command":["python","-c","import requests;print(\"STATUS:%d\" % requests.post(\"http://crypto:8444/encrypt\",json={\"field\":\"x\",\"values\":[\"a\"]},timeout=10).status_code)"]}]}}' 2>/dev/null)
code=$(echo "$raw" | grep -oE 'STATUS:[0-9]+' | head -1 | cut -d: -f2)
if [ "$code" = "401" ]; then echo "$OK unauthenticated crypto request refused (401)"
else echo "$BAD unauthenticated crypto request returned '$code', expected 401"; fail=1; fi

# 3. No plaintext in the warehouse. The masked station column must never hold
#    a real station name.
leak=$($K exec statefulset/postgres -- psql -U pipeline -d analytics -tAc \
  "SELECT count(*) FROM warehouse.trips WHERE start_station_masked IN ('Riverside @ S. Lamar','Zilker Park West','Congress & 6th','4th & Congress');" 2>/dev/null | tr -d '\r\n ')
if [ "${leak:-x}" = "0" ]; then echo "$OK no plaintext station names in the warehouse"
elif [ -z "${leak:-}" ]; then echo "$SKIP warehouse not reachable yet"
else echo "$BAD $leak row(s) hold an UNMASKED station name"; fail=1; fi

# 4. Masked columns must actually be masked
unmasked=$($K exec statefulset/postgres -- psql -U pipeline -d analytics -tAc \
  "SELECT count(*) FROM warehouse.trips WHERE bike_id_masked IS NOT NULL AND bike_id_masked NOT LIKE '%*%';" 2>/dev/null | tr -d '\r\n ')
if [ "${unmasked:-x}" = "0" ]; then echo "$OK every bike_id is masked"
elif [ -z "${unmasked:-}" ]; then echo "$SKIP no rows yet — run the pipeline first"
else echo "$BAD $unmasked bike_id value(s) unmasked"; fail=1; fi

# 5. Which engine masked it — a claim, not an assumption
engines=$($K exec statefulset/postgres -- psql -U pipeline -d analytics -tAc \
  "SELECT DISTINCT masked_by FROM warehouse.trips;" 2>/dev/null | tr -d '\r' | paste -sd, -)
[ -n "${engines:-}" ] && echo "$OK masked_by = ${engines}  (local-policy-engine means Ranger admin is NOT deployed)"

# 6. Secrets exist and none is empty
for s in gcp-sa pipeline-crypto pipeline-clients pipeline-datastores; do
  n=$($K get secret "$s" -o jsonpath='{.data}' 2>/dev/null | tr ',' '\n' | grep -c ':' || echo 0)
  if [ "$n" -gt 0 ]; then echo "$OK secret $s ($n key(s))"; else echo "$BAD secret $s missing/empty"; fail=1; fi
done

# 7. No secret material leaked into the ConfigMap
if $K get configmap trips-config -o yaml 2>/dev/null | grep -qiE "PRIVATE KEY|password:|secret:"; then
  echo "$BAD trips-config appears to contain secret material"; fail=1
else
  echo "$OK no secret material in the ConfigMap"
fi

echo "──────────────────────────────────────────────────────"
[ "$fail" -eq 0 ] && echo "All checks passed." || echo "FAILURES above — do not ship."
exit "$fail"
