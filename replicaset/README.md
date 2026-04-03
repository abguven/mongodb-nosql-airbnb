# ReplicaSet — High Availability Deployment

This document describes the steps to deploy a 3-node MongoDB ReplicaSet locally using Docker Compose and migrate the existing data into it.

## Prerequisites

- Docker and Docker Compose installed
- MongoDB Database Tools (`mongodump`, `mongorestore`) installed locally
- A `listings` dump available in `replicaset/scripts/dump_noscites/` (produced via `mongodump` at the end of Phase 1)

## Step 1 — Start the Docker Infrastructure

The full infrastructure is defined in `docker-compose.yml`. It deploys:

- **3 MongoDB containers** (`mongo1`, `mongo2`, `mongo3`) configured as ReplicaSet `rs_noscites`
- **1 client container** (`mongo-client`) to run administration scripts inside the Docker network

```bash
# If not already started from the main README
docker compose up -d
```

## Step 2 — Initialize the ReplicaSet

Once the containers are running, the `mongod` instances are not yet aware of each other. The ReplicaSet must be explicitly initialized.

1. Enter the client container:

    ```bash
    docker exec -it mongo-client bash
    ```

2. Connect to one of the nodes:

    ```bash
    mongosh --host mongo1
    ```

3. Run the initialization script:

    The `init-replicaset.mongodb.js` script automates the configuration. It is idempotent and waits for a `PRIMARY` election before completing.

    ```javascript
    load('/scripts/init-replicaset.mongodb.js')
    ```

    At the end of this step, the ReplicaSet is fully operational.

## Step 3 — Restore Data

> The `dump_noscites/` folder was produced during Phase 1. Copy it into `replicaset/scripts/` before running the restore.

From inside the `mongo-client` container, exit `mongosh` first then run the restore from bash. The connection string must include all members and the ReplicaSet name so `mongorestore` automatically finds the `PRIMARY`.

```bash
# Exit mongosh if still open
exit

mongorestore --uri="mongodb://mongo1:27017,mongo2:27017,mongo3:27017/?replicaSet=rs_noscites" \
             --db="short_term_rentals" \
             --collection="listings" \
             "/scripts/dump_noscites/short_term_rentals/listings.bson"
```

## Step 4 — Verify

**Check document count on the PRIMARY:**

```bash
mongosh "mongodb://mongo1:27017,mongo2:27017,mongo3:27017/?replicaSet=rs_noscites"

use short_term_rentals
db.listings.countDocuments()
```

**Verify replication on a SECONDARY** — from the same connection, switch the read preference to force reads onto a secondary node:

```js
db.getMongo().setReadPref("secondary")
use short_term_rentals
db.listings.countDocuments()
```

Both counts should return `105858`. If the secondary count matches the primary, replication is working correctly.

## 🎓 Bonus — Real-Time Role Monitoring

`scripts/monitoring_script.sh` polls the ReplicaSet every 2 seconds and displays the current role of each node (PRIMARY / SECONDARY). Run it in a separate terminal while testing failover scenarios (e.g. stopping `mongo1`) to observe the automatic PRIMARY election in real time — a great way to understand how ReplicaSet high availability works in practice.

```bash
bash scripts/monitoring_script.sh
```
