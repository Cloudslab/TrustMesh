#!/bin/bash

# Function to wait for a job to complete
wait_for_job() {
    local job_prefix=$1
    echo "Waiting for job $job_prefix to complete..."
    while true; do
        job_name=$(kubectl get jobs --selector=job-name=$job_prefix -o jsonpath='{.items[*].metadata.name}')
        if [ -n "$job_name" ]; then
            status=$(kubectl get job $job_name -o jsonpath='{.status.conditions[?(@.type=="Complete")].status}')
            if [ "$status" == "True" ]; then
                echo "Job $job_name completed successfully."
                break
            fi
        fi
        sleep 5
    done
}

# Function to check if a node exists in the cluster
check_node_exists() {
    local node_name=$1
    kubectl get nodes | grep -q "$node_name"
    return $?
}

generate_network_key_zmq() {
    local key_dir="kubernetes-manifests/generated/network_keys"
    mkdir -p "$key_dir"

    # Install dependencies and compile curve_keygen
    sudo apt-get update && sudo apt-get install -y g++ libzmq3-dev
    wget https://raw.githubusercontent.com/zeromq/libzmq/master/tools/curve_keygen.cpp
    g++ curve_keygen.cpp -o curve_keygen -lzmq

    # Generate keys
    key_output=$(./curve_keygen)
    public_key=$(echo "$key_output" | grep "CURVE PUBLIC KEY" -A 1 | tail -n 1)
    private_key=$(echo "$key_output" | grep "CURVE SECRET KEY" -A 1 | tail -n 1)

    # Save keys to files (for reference, consider removing in production)
    echo "$public_key" > "$key_dir/network_public.key"
    echo "$private_key" > "$key_dir/network_private.key"

    # Create a Kubernetes Secret YAML
    cat << EOF > kubernetes-manifests/generated/network-key-secret.yaml
apiVersion: v1
kind: Secret
metadata:
  name: network-key-secret
type: Opaque
stringData:
  network_private_key: "${private_key}"
  network_public_key: "${public_key}"
EOF

    echo "Network key Secret YAML has been created at kubernetes-manifests/generated/network-key-secret.yaml"

    kubectl apply -f kubernetes-manifests/generated/network-key-secret.yaml

    # Clean up
    rm curve_keygen curve_keygen.cpp
}

generate_ssl_certificates() {
    local num_nodes=$1
    local cert_dir="kubernetes-manifests/generated/certs"

    mkdir -p "$cert_dir"

    # Generate CA key and certificate
    openssl genrsa -out "$cert_dir/ca.key" 4096
    openssl req -new -x509 -key "$cert_dir/ca.key" -out "$cert_dir/ca.crt" -days 365 -subj "/CN=CouchDB CA"
    chmod 600 "$cert_dir/ca.key" "$cert_dir/ca.crt"

    # Generate certificates for each node
    for ((i=0; i<num_nodes; i++)); do
        openssl genrsa -out "$cert_dir/node$i.key" 2048
        openssl req -new -key "$cert_dir/node$i.key" -out "$cert_dir/node$i.csr" \
            -subj "/CN=couchdb-$i.default.svc.cluster.local"

        cat > "$cert_dir/node$i.ext" << EOF
subjectAltName = DNS:couchdb-$i.default.svc.cluster.local, \
                 DNS:couchdb@couchdb-$i.default.svc.cluster.local, \
                 DNS:couchdb-$i, \
                 DNS:localhost
EOF

        openssl x509 -req -in "$cert_dir/node$i.csr" -CA "$cert_dir/ca.crt" -CAkey "$cert_dir/ca.key" \
            -CAcreateserial -out "$cert_dir/node$i.crt" -days 365 -extfile "$cert_dir/node$i.ext"

        chmod 600 "$cert_dir/node$i.key" "$cert_dir/node$i.csr" "$cert_dir/node$i.crt" "$cert_dir/node$i.ext"
    done

    # Create Kubernetes secret for certificates
    kubectl create secret generic couchdb-certs \
        --from-file="$cert_dir/ca.crt" \
        $(for ((i=0; i<num_nodes; i++)); do echo "--from-file=node${i}_crt=$cert_dir/node$i.crt --from-file=node${i}_key=$cert_dir/node$i.key"; done)
}

generate_couchdb_yaml() {
    local num_compute_nodes=$1
    local yaml_content="apiVersion: v1
kind: List

items:"

    # Generate PVCs
    for ((i=0; i<num_compute_nodes; i++)); do
        yaml_content+="
  - apiVersion: v1
    kind: PersistentVolumeClaim
    metadata:
      name: couchdb${i}-data
    spec:
      accessModes:
        - ReadWriteOnce
      resources:
        requests:
          storage: 1Gi"
    done

    # Generate Deployments
    for ((i=0; i<num_compute_nodes; i++)); do
        yaml_content+="
  - apiVersion: apps/v1
    kind: Deployment
    metadata:
      name: couchdb-${i}
    spec:
      selector:
        matchLabels:
          app: couchdb-${i}
      replicas: 1
      template:
        metadata:
          labels:
            app: couchdb-${i}
        spec:
          nodeSelector:
            kubernetes.io/hostname: compute-node-$((i+1))
          initContainers:
            - name: init-config
              image: busybox
              command: ['sh', '-c', 'cp /tmp/couchdb-config/* /opt/couchdb/etc/local.d/ && echo \"Config files copied\" && ls -la /opt/couchdb/etc/local.d']
              volumeMounts:
                - name: couchdb-config
                  mountPath: /tmp/couchdb-config
                - name: config-storage
                  mountPath: /opt/couchdb/etc/local.d
          containers:
            - name: couchdb
              image: couchdb:latest
              ports:
                - containerPort: 5984
                - containerPort: 6984
              env:
                - name: COUCHDB_USER
                  valueFrom:
                    secretKeyRef:
                      name: couchdb-secrets
                      key: COUCHDB_USER
                - name: COUCHDB_PASSWORD
                  valueFrom:
                    secretKeyRef:
                      name: couchdb-secrets
                      key: COUCHDB_PASSWORD
                - name: COUCHDB_SECRET
                  valueFrom:
                    secretKeyRef:
                      name: couchdb-secrets
                      key: COUCHDB_SECRET
                - name: ERL_FLAGS
                  value: \"-setcookie jT7egojgnPLzOncq9MQUzqwqHm6ZiPUU7xJfFLA8MA -couch_log level debug -kernel inet_dist_listen_min 9100 -kernel inet_dist_listen_max 9200\"
                - name: NODENAME
                  value: \"couchdb-${i}.default.svc.cluster.local\"
              volumeMounts:
                - name: config-storage
                  mountPath: /opt/couchdb/etc/local.d
                - name: couchdb-data
                  mountPath: /opt/couchdb/data
                - name: couchdb-certs
                  mountPath: /home/couchdb-certs
          volumes:
            - name: couchdb-data
              persistentVolumeClaim:
                claimName: couchdb${i}-data
            - name: couchdb-config
              configMap:
                name: couchdb-config-${i}
            - name: config-storage
              emptyDir: {}
            - name: couchdb-certs
              secret:
                secretName: couchdb-certs"
    done

    # Generate Services
    for ((i=0; i<num_compute_nodes; i++)); do
        yaml_content+="
  - apiVersion: v1
    kind: Service
    metadata:
      name: couchdb-${i}
    spec:
      clusterIP: None
      selector:
        app: couchdb-${i}
      ports:
        - name: http
          port: 5984
          targetPort: 5984
        - name: https
          port: 6984
          targetPort: 6984"
    done

    for ((i=0; i<num_compute_nodes; i++)); do
    # Generate ConfigMap for CouchDB configuration
    yaml_content+="
  - apiVersion: v1
    kind: ConfigMap
    metadata:
      name: couchdb-config-${i}
    data:
      local.ini: |
        [couchdb]
        uuid = couch

        [chttpd]
        port = 5984
        bind_address = 0.0.0.0
        enable_cors = true

        [cors]
        origins = *
        credentials = true
        methods = GET, PUT, POST, HEAD, DELETE
        headers = accept, authorization, content-type, origin, referer

        [cluster]
        enable_ssl = true
        q = 8
        n = 3

        [httpd]
        enable_cors = true
        
        [log]
        level = debug

        [ssl]
        enable = true
        port = 6984
        cert_file = /home/couchdb-certs/node${i}_crt
        key_file = /home/couchdb-certs/node${i}_key
        cacert_file = /home/couchdb-certs/ca.crt
        certfile = /home/couchdb-certs/node${i}_crt
        keyfile = /home/couchdb-certs/node${i}_key
        cacertfile = /home/couchdb-certs/ca.crt
        verify_ssl = true
        tls_versions = [tlsv1, 'tlsv1.1', 'tlsv1.2', 'tlsv1.3']
        ssl_log_level = debug
        
        [ssl_dist_opts]
        certfile = /home/couchdb-certs/node${i}_crt
        keyfile = /home/couchdb-certs/node${i}_key
        cacertfile = /home/couchdb-certs/ca.crt
        verify = verify_peer
        server_name_indication = disable
      ssl_dist.conf: |
        [{server, [{certfile, \"/home/couchdb-certs/node${i}_crt\"},
                   {keyfile, \"/home/couchdb-certs/node${i}_key\"},
                   {cacertfile, \"/home/couchdb-certs/ca.crt\"},
                   {verify, verify_peer},
                   {fail_if_no_peer_cert, false}]},
         {client, [{certfile, \"/home/couchdb-certs/node${i}_crt\"},
                   {keyfile, \"/home/couchdb-certs/node${i}_key\"},
                   {cacertfile, \"/home/couchdb-certs/ca.crt\"},
                   {verify, verify_peer}]}]."

      done

    # Generate CouchDB Cluster Setup Job
    yaml_content+="
  - apiVersion: batch/v1
    kind: Job
    metadata:
      name: couchdb-setup
    spec:
      template:
        metadata:
          name: couchdb-setup
        spec:
          restartPolicy: OnFailure
          containers:
            - name: couchdb-setup
              image: curlimages/curl:latest
              command:
                - /bin/sh
              args:
                - -c
                - |
                  echo \"Starting CouchDB cluster setup\" &&
                  for i in \$(seq 0 $((num_compute_nodes-1))); do
                    echo \"https://\${COUCHDB_USER}:\${COUCHDB_PASSWORD}@couchdb-\${i}.default.svc.cluster.local:6984\"
                    until curl --cacert /certs/ca.crt --cert /certs/node\${i}_crt --key /certs/node\${i}_key -s \"https://\${COUCHDB_USER}:\${COUCHDB_PASSWORD}@couchdb-\${i}.default.svc.cluster.local:6984\" > /dev/null; do
                      echo \"Waiting for CouchDB on couchdb-\${i} to be ready...\"
                      sleep 5
                    done
                    echo \"CouchDB on couchdb-\${i} is ready\"
                  done &&
                  echo \"Setting up CouchDB cluster using manual method (Cluster Setup API doesn't support SSL)\" &&
                  echo \"Step 1: Initialize cluster mode on coordinator node\" &&
                  init_response=\$(curl --cacert /certs/ca.crt --cert /certs/node0_crt --key /certs/node0_key -X POST \"https://\${COUCHDB_USER}:\${COUCHDB_PASSWORD}@couchdb-0.default.svc.cluster.local:6984/_cluster_setup\" -H 'Content-Type: application/json' -d \"{\\\"action\\\": \\\"enable_cluster\\\", \\\"bind_address\\\": \\\"0.0.0.0\\\", \\\"username\\\": \\\"\${COUCHDB_USER}\\\", \\\"password\\\": \\\"\${COUCHDB_PASSWORD}\\\", \\\"node_count\\\": \\\"$num_compute_nodes\\\"}\")
                  echo \"Coordinator cluster initialization response: \${init_response}\" &&
                  sleep 3 &&
                  echo \"Step 2: Initialize cluster mode on remote nodes\" &&
                  for num in \$(seq 1 $((num_compute_nodes-1))); do
                    echo \"Initializing cluster mode on couchdb-\${num}...\"
                    remote_init=\$(curl --cacert /certs/ca.crt --cert /certs/node\${num}_crt --key /certs/node\${num}_key -X POST \"https://\${COUCHDB_USER}:\${COUCHDB_PASSWORD}@couchdb-\${num}.default.svc.cluster.local:6984/_cluster_setup\" -H 'Content-Type: application/json' -d \"{\\\"action\\\": \\\"enable_cluster\\\", \\\"bind_address\\\": \\\"0.0.0.0\\\", \\\"username\\\": \\\"\${COUCHDB_USER}\\\", \\\"password\\\": \\\"\${COUCHDB_PASSWORD}\\\", \\\"node_count\\\": \\\"$num_compute_nodes\\\"}\")
                    echo \"Remote node couchdb-\${num} initialization response: \${remote_init}\"
                    sleep 2
                  done &&
                  sleep 5 &&
                  echo \"Step 3: Adding remote nodes to cluster using _nodes API (non-SSL method)\" &&
                  for num in \$(seq 1 $((num_compute_nodes-1))); do
                    echo \"Adding couchdb-\${num} to cluster via _nodes API...\"
                    add_response=\$(curl --cacert /certs/ca.crt --cert /certs/node0_crt --key /certs/node0_key -X PUT \"https://\${COUCHDB_USER}:\${COUCHDB_PASSWORD}@couchdb-0.default.svc.cluster.local:6984/_nodes/couchdb@couchdb-\${num}.default.svc.cluster.local\" -d '{}' -H 'Content-Type: application/json')
                    echo \"Add node couchdb-\${num} response: \${add_response}\"
                    sleep 3
                  done &&
                  echo \"Waiting for cluster to stabilize...\" &&
                  sleep 10 &&
                  echo \"Checking cluster membership\" &&
                  membership=\$(curl --cacert /certs/ca.crt --cert /certs/node0_crt --key /certs/node0_key -s -X GET \"https://\${COUCHDB_USER}:\${COUCHDB_PASSWORD}@couchdb-0.default.svc.cluster.local:6984/_membership\") &&
                  echo \"Cluster membership: \${membership}\" &&
                  echo \"Creating system databases (_replicator, _users, _global_changes)\" &&
                  system_dbs=\"_replicator _users _global_changes\"
                  for db in \$system_dbs; do
                    echo \"Creating system database: \$db\"
                    response=\$(curl --cacert /certs/ca.crt --cert /certs/node0_crt --key /certs/node0_key -s -X PUT \"https://\${COUCHDB_USER}:\${COUCHDB_PASSWORD}@couchdb-0.default.svc.cluster.local:6984/\$db\")
                    echo \"Creating \$db response: \${response}\"
                  done &&
                  echo \"Creating application databases (\${RESOURCE_REGISTRY_DB} and \${TASK_DATA_DB})\" &&
                  response=\$(curl --cacert /certs/ca.crt --cert /certs/node0_crt --key /certs/node0_key -s -X PUT \"https://\${COUCHDB_USER}:\${COUCHDB_PASSWORD}@couchdb-0.default.svc.cluster.local:6984/\${RESOURCE_REGISTRY_DB}\") &&
                  echo \"Creating \${RESOURCE_REGISTRY_DB} response: \${response}\" &&
                  response=\$(curl --cacert /certs/ca.crt --cert /certs/node0_crt --key /certs/node0_key -s -X PUT \"https://\${COUCHDB_USER}:\${COUCHDB_PASSWORD}@couchdb-0.default.svc.cluster.local:6984/\${TASK_DATA_DB}\") &&
                  echo \"Creating \${TASK_DATA_DB} response: \${response}\" &&
                  echo \"Waiting for \${RESOURCE_REGISTRY_DB} & \${TASK_DATA_DB} to be available on all nodes\" &&
                  for db in \${RESOURCE_REGISTRY_DB} \${TASK_DATA_DB}; do
                    for i in \$(seq 0 $((num_compute_nodes-1))); do
                      until curl --cacert /certs/ca.crt --cert /certs/node\${i}_crt --key /certs/node\${i}_key -s \"https://\${COUCHDB_USER}:\${COUCHDB_PASSWORD}@couchdb-\${i}.default.svc.cluster.local:6984/\${db}\" | grep -q \"\${db}\"; do
                        echo \"Waiting for \${db} on couchdb-\${i}...\"
                        sleep 5
                      done
                      echo \"\${db} is available on couchdb-\${i}\"
                    done
                  done &&
                  echo \"CouchDB cluster setup completed and \${RESOURCE_REGISTRY_DB}, \${TASK_DATA_DB} databases are available on all nodes\"
              env:
                - name: RESOURCE_REGISTRY_DB
                  value: \"resource_registry\"
                - name: TASK_DATA_DB
                  value: \"task_data\"
                - name: COUCHDB_USER
                  valueFrom:
                    secretKeyRef:
                      name: couchdb-secrets
                      key: COUCHDB_USER
                - name: COUCHDB_PASSWORD
                  valueFrom:
                    secretKeyRef:
                      name: couchdb-secrets
                      key: COUCHDB_PASSWORD
              volumeMounts:
                - name: couchdb-certs
                  mountPath: /certs
          volumes:
            - name: couchdb-certs
              secret:
                secretName: couchdb-certs"

    echo "$yaml_content"
}

# Function to generate blockchain network deployment YAML
generate_blockchain_network_yaml() {
    local num_compute_nodes=$1
    local num_iot_nodes=$2
    local yaml_content="apiVersion: v1
kind: List

items:"

    # Generate compute Node Deployments and Services
    for ((i=0; i<num_compute_nodes; i++)); do
        local hostname="compute-node-$((i+1))"
        local deployment_name="pbft-$i"
        local service_name="sawtooth-$i"

        yaml_content+="
  # --------------------------=== $hostname ===--------------------------

  - apiVersion: apps/v1
    kind: Deployment
    metadata:
      name: $deployment_name
    spec:
      selector:
        matchLabels:
          name: $deployment_name
      replicas: 1
      template:
        metadata:
          labels:
            name: $deployment_name
        spec:
          nodeSelector:
            kubernetes.io/hostname: $hostname
          volumes:
            - name: proc
              hostPath:
                path: /proc
            - name: sys
              hostPath:
                path: /sys
            - name: couchdb-certs
              secret:
                secretName: couchdb-certs
          initContainers:
            - name: wait-for-registry
              image: busybox
              command: [ 'sh', '-c', 'until nc -z sawtooth-registry 5000; do echo waiting for sawtooth-registry; sleep 2; done;' ]
            - name: wait-for-couchdb-setup
              image: curlimages/curl:latest
              command:
                - 'sh'
                - '-c'
                - |
                  for db in \${RESOURCE_REGISTRY_DB} \${TASK_DATA_DB}; do
                    for i in \$(seq 0 $((num_compute_nodes-1))); do
                      until curl --cacert /certs/ca.crt --cert /certs/node\${i}_crt --key /certs/node\${i}_key -s \"https://\${COUCHDB_USER}:\${COUCHDB_PASSWORD}@couchdb-\${i}.default.svc.cluster.local:6984/\${db}\" | grep -q \"\${db}\"; do
                        echo \"Waiting for \${db} on couchdb-\${i}...\"
                        sleep 5
                      done
                      echo \"\${db} is available on couchdb-\${i}\"
                    done
                  done &&
                  echo \"CouchDB cluster setup completed and \${RESOURCE_REGISTRY_DB} & \${TASK_DATA_DB} is available on all nodes\"
              env:
                - name: RESOURCE_REGISTRY_DB
                  value: \"resource_registry\"
                - name: TASK_DATA_DB
                  value: \"task_data\"
                - name: COUCHDB_USER
                  valueFrom:
                    secretKeyRef:
                      name: couchdb-secrets
                      key: COUCHDB_USER
                - name: COUCHDB_PASSWORD
                  valueFrom:
                    secretKeyRef:
                      name: couchdb-secrets
                      key: COUCHDB_PASSWORD
              volumeMounts:
                - name: couchdb-certs
                  mountPath: /certs
          containers:
            - name: peer-registry-tp
              image: murtazahr/peer-registry-tp:latest
              env:
                - name: MAX_UPDATES_PER_NODE
                  value: \"100\"
                - name: VALIDATOR_URL
                  value: \"tcp://$service_name:4004\"

            - name: docker-image-tp
              image: murtazahr/docker-image-tp:latest
              env:
                - name: VALIDATOR_URL
                  value: \"tcp://$service_name:4004\"

            - name: dependency-management-tp
              image: murtazahr/dependency-management-tp:latest
              env:
                - name: VALIDATOR_URL
                  value: \"tcp://$service_name:4004\"

            - name: schedule-status-update-tp
              image: murtazahr/schedule-status-update-tp:latest
              env:
                - name: VALIDATOR_URL
                  value: \"tcp://$service_name:4004\"

            - name: iot-data-tp
              image: murtazahr/iot-data-tp:latest
              env:
                - name: VALIDATOR_URL
                  value: \"tcp://$service_name:4004\"
                - name: COUCHDB_HOST
                  value: \"couchdb-$i.default.svc.cluster.local:6984\"
                - name: COUCHDB_USER
                  valueFrom:
                    secretKeyRef:
                      name: couchdb-secrets
                      key: COUCHDB_USER
                - name: COUCHDB_PASSWORD
                  valueFrom:
                    secretKeyRef:
                      name: couchdb-secrets
                      key: COUCHDB_PASSWORD
                - name: COUCHDB_SSL_CERT
                  valueFrom:
                    secretKeyRef:
                      name: couchdb-certs
                      key: node${i}_crt
                - name: COUCHDB_SSL_KEY
                  valueFrom:
                    secretKeyRef:
                      name: couchdb-certs
                      key: node${i}_key
                - name: COUCHDB_SSL_CA
                  valueFrom:
                    secretKeyRef:
                      name: couchdb-certs
                      key: ca.crt
                - name: REDIS_PASSWORD
                  valueFrom:
                    secretKeyRef:
                      name: redis-password
                      key: password
                - name: REDIS_SSL_CERT
                  valueFrom:
                    secretKeyRef:
                      name: redis-certificates
                      key: redis.crt
                - name: REDIS_SSL_KEY
                  valueFrom:
                    secretKeyRef:
                      name: redis-certificates
                      key: redis.key
                - name: REDIS_SSL_CA
                  valueFrom:
                    secretKeyRef:
                      name: redis-certificates
                      key: ca.crt

            - name: scheduling-request-tp
              image: murtazahr/scheduling-request-tp:latest
              env:
                - name: VALIDATOR_URL
                  value: \"tcp://$service_name:4004\"
                - name: REDIS_PASSWORD
                  valueFrom:
                    secretKeyRef:
                      name: redis-password
                      key: password
                - name: REDIS_SSL_CERT
                  valueFrom:
                    secretKeyRef:
                      name: redis-certificates
                      key: redis.crt
                - name: REDIS_SSL_KEY
                  valueFrom:
                    secretKeyRef:
                      name: redis-certificates
                      key: redis.key
                - name: REDIS_SSL_CA
                  valueFrom:
                    secretKeyRef:
                      name: redis-certificates
                      key: ca.crt

            - name: schedule-confirmation-tp
              image: murtazahr/schedule-confirmation-tp:latest
              env:
                - name: VALIDATOR_URL
                  value: \"tcp://$service_name:4004\"

            - name: sawtooth-pbft-engine
              image: hyperledger/sawtooth-pbft-engine:chime
              command:
                - bash
              args:
                - -c
                - \"pbft-engine -vv --connect tcp://\$HOSTNAME:5050\"

            - name: sawtooth-settings-tp
              image: hyperledger/sawtooth-settings-tp:chime
              command:
                - bash
              args:
                - -c
                - \"settings-tp -vv -C tcp://\$HOSTNAME:4004\"

            - name: sawtooth-rest-api
              image: hyperledger/sawtooth-rest-api:chime
              ports:
                - name: api
                  containerPort: 8008
              command:
                - bash
              args:
                - -c
                - \"sawtooth-rest-api -vv -C tcp://\$HOSTNAME:4004 -B 0.0.0.0:8008\"
              readinessProbe:
                httpGet:
                  path: /status
                  port: 8008
                initialDelaySeconds: 15    # Time to wait after container starts before checking
                periodSeconds: 30          # How often to check
                timeoutSeconds: 30          # How long to wait for a response
                failureThreshold: 3        # How many failures before marking unready
                successThreshold: 1        # How many successes before marking ready

            - name: sawtooth-shell
              image: hyperledger/sawtooth-shell:chime
              command:
                - bash
              args:
                - -c
                - \"sawtooth keygen && tail -f /dev/null\"

            - name: compute-node
              resources: {}
              image: murtazahr/compute-node:latest
              securityContext:
                privileged: true
              volumeMounts:
                - name: proc
                  mountPath: /host/proc
                  readOnly: true
                - name: sys
                  mountPath: /host/sys
                  readOnly: true
              env:
                - name: REGISTRY_URL
                  value: \"sawtooth-registry:5000\"
                - name: VALIDATOR_URL
                  value: \"tcp://$service_name:4004\"
                - name: NODE_ID
                  value: \"sawtooth-compute-node-$i\"
                - name: COUCHDB_HOST
                  value: \"couchdb-$i.default.svc.cluster.local:6984\"
                - name: RESOURCE_UPDATE_INTERVAL
                  value: \"30\"
                - name: RESOURCE_UPDATE_BATCH_SIZE
                  value: \"500\"
                - name: COUCHDB_USER
                  valueFrom:
                    secretKeyRef:
                      name: couchdb-secrets
                      key: COUCHDB_USER
                - name: COUCHDB_PASSWORD
                  valueFrom:
                    secretKeyRef:
                      name: couchdb-secrets
                      key: COUCHDB_PASSWORD
                - name: COUCHDB_SSL_CERT
                  valueFrom:
                    secretKeyRef:
                      name: couchdb-certs
                      key: node${i}_crt
                - name: COUCHDB_SSL_KEY
                  valueFrom:
                    secretKeyRef:
                      name: couchdb-certs
                      key: node${i}_key
                - name: COUCHDB_SSL_CA
                  valueFrom:
                    secretKeyRef:
                      name: couchdb-certs
                      key: ca.crt
                - name: REDIS_PASSWORD
                  valueFrom:
                    secretKeyRef:
                      name: redis-password
                      key: password
                - name: REDIS_SSL_CERT
                  valueFrom:
                    secretKeyRef:
                      name: redis-certificates
                      key: redis.crt
                - name: REDIS_SSL_KEY
                  valueFrom:
                    secretKeyRef:
                      name: redis-certificates
                      key: redis.key
                - name: REDIS_SSL_CA
                  valueFrom:
                    secretKeyRef:
                      name: redis-certificates
                      key: ca.crt
                - name: IS_NEW_ADDITION
                  value: \"false\"

            - name: sawtooth-validator
              image: hyperledger/sawtooth-validator:chime
              ports:
                - name: tp
                  containerPort: 4004
                - name: consensus
                  containerPort: 5050
                - name: validators
                  containerPort: 8800"

        if [ "$i" -eq 0 ]; then
            local pbft_members=$(for ((j=0; j<num_compute_nodes; j++)); do
                echo -n "\\\"\$pbft${j}pub\\\""
                if [ $j -lt $((num_compute_nodes-1)) ]; then
                    echo -n ","
                fi
            done)

            yaml_content+="
              envFrom:
                - configMapRef:
                    name: keys-config
              env:
                - name: NETWORK_PRIVATE_KEY
                  valueFrom:
                    secretKeyRef:
                      name: network-key-secret
                      key: network_private_key
                - name: NETWORK_PUBLIC_KEY
                  valueFrom:
                    secretKeyRef:
                      name: network-key-secret
                      key: network_public_key
              command:
                - bash
              args:
                - -c
                - |
                  if [ ! -e /etc/sawtooth/keys/validator.priv ]; then
                    echo \$pbft0priv > /etc/sawtooth/keys/validator.priv
                    echo \$pbft0pub > /etc/sawtooth/keys/validator.pub
                  fi &&
                  if [ ! -e /root/.sawtooth/keys/my_key.priv ]; then
                    sawtooth keygen my_key
                  fi &&
                  if [ ! -e config-genesis.batch ]; then
                    sawset genesis -k /root/.sawtooth/keys/my_key.priv -o config-genesis.batch
                  fi &&
                  sleep 30 &&
                  echo sawtooth.consensus.pbft.members=[\"${pbft_members}\"] &&
                  if [ ! -e config.batch ]; then
                    sawset proposal create -k /root/.sawtooth/keys/my_key.priv sawtooth.consensus.algorithm.name=pbft sawtooth.consensus.algorithm.version=1.0 sawtooth.consensus.pbft.members=[\"${pbft_members}\"] sawtooth.publisher.max_batches_per_block=100 sawtooth.validator.state_pruning_enabled=false sawtooth.validator.max_database_size_mb=15000 sawtooth.validation.block_validation_rules=NofX:1,block_info sawtooth.consensus.pbft.block_publishing_delay=2000 sawtooth.validator.transaction_queue_max_length=10000 -o config.batch
                  fi && if [ ! -e /var/lib/sawtooth/genesis.batch ]; then
                    sawadm genesis config-genesis.batch config.batch
                  fi &&
                  echo \"network_public_key = '\${NETWORK_PUBLIC_KEY}'\" > /etc/sawtooth/validator.toml &&
                  echo \"network_private_key = '\${NETWORK_PRIVATE_KEY}'\" >> /etc/sawtooth/validator.toml &&
                  chown root:sawtooth /etc/sawtooth/validator.toml &&
                  chmod 640 /etc/sawtooth/validator.toml &&
                  sawtooth-validator -vv --endpoint tcp://\$SAWTOOTH_0_SERVICE_HOST:8800 --bind component:tcp://eth0:4004 --bind consensus:tcp://eth0:5050 --bind network:tcp://eth0:8800 --network-auth trust --scheduler parallel --peering static --maximum-peer-connectivity 10000"
        else
            yaml_content+="
              env:
                - name: pbft${i}priv
                  valueFrom:
                    configMapKeyRef:
                      name: keys-config
                      key: pbft${i}priv
                - name: pbft${i}pub
                  valueFrom:
                    configMapKeyRef:
                      name: keys-config
                      key: pbft${i}pub
                - name: NETWORK_PRIVATE_KEY
                  valueFrom:
                    secretKeyRef:
                      name: network-key-secret
                      key: network_private_key
                - name: NETWORK_PUBLIC_KEY
                  valueFrom:
                    secretKeyRef:
                      name: network-key-secret
                      key: network_public_key
              command:
                - bash
              args:
                - -c
                - |
                  if [ ! -e /etc/sawtooth/keys/validator.priv ]; then
                    echo \$pbft${i}priv > /etc/sawtooth/keys/validator.priv
                    echo \$pbft${i}pub > /etc/sawtooth/keys/validator.pub
                  fi &&
                  sawtooth keygen my_key &&
                  echo \"network_public_key = '\${NETWORK_PUBLIC_KEY}'\" > /etc/sawtooth/validator.toml &&
                  echo \"network_private_key = '\${NETWORK_PRIVATE_KEY}'\" >> /etc/sawtooth/validator.toml &&
                  chown root:sawtooth /etc/sawtooth/validator.toml &&
                  chmod 640 /etc/sawtooth/validator.toml &&
                  sawtooth-validator -vv --endpoint tcp://\$SAWTOOTH_${i}_SERVICE_HOST:8800 --bind component:tcp://eth0:4004 --bind consensus:tcp://eth0:5050 --bind network:tcp://eth0:8800 --network-auth trust --scheduler parallel --peering static --maximum-peer-connectivity 10000 $(for ((j=0; j<i; j++)); do echo -n "--peers tcp://\$SAWTOOTH_${j}_SERVICE_HOST:8800 "; done)"
        fi

        yaml_content+="

  - apiVersion: v1
    kind: Service
    metadata:
      name: $service_name
    spec:
      type: ClusterIP
      selector:
        name: $deployment_name
      ports:
        - name: \"4004\"
          protocol: TCP
          port: 4004
          targetPort: 4004
        - name: \"5050\"
          protocol: TCP
          port: 5050
          targetPort: 5050
        - name: \"8008\"
          protocol: TCP
          port: 8008
          targetPort: 8008
        - name: \"8080\"
          protocol: TCP
          port: 8080
          targetPort: 8080
        - name: \"5000\"
          protocol: TCP
          port: 5000
          targetPort: 5000
        - name: \"8800\"
          protocol: TCP
          port: 8800
          targetPort: 8800"
    done

    # Generate IoT Node Deployments
    for ((i=0; i<num_iot_nodes; i++)); do
        local hostname="iot-node-$((i+1))"
        local deployment_name="iot-$i"
        local service_name="iot-$i"

        yaml_content+="

  # -------------------------=== iot-node-$((i+1)) ===------------------

  - apiVersion: apps/v1
    kind: Deployment
    metadata:
      name: $deployment_name
    spec:
      selector:
        matchLabels:
          name: $deployment_name
      replicas: 1
      template:
        metadata:
          labels:
            name: $deployment_name
        spec:
          nodeSelector:
            kubernetes.io/hostname: iot-node-$((i+1))
          containers:
            - name: iot-node
              image: murtazahr/iot-node:latest
              env:
                - name: VALIDATOR_URLS
                  value: \"$(for ((j=0; j<num_compute_nodes; j++)); do echo -n "tcp://sawtooth-$j:4004"; if [ $j -lt $((num_compute_nodes-1)) ]; then echo -n ,; fi; done)\"
                - name: IOT_URL
                  value: \"tcp://$service_name\"

  - apiVersion: v1
    kind: Service
    metadata:
      name: $service_name
    spec:
      type: ClusterIP
      selector:
        name: $deployment_name
      ports:
        - name: \"5555\"
          protocol: TCP
          port: 5555
          targetPort: 5555"

    done

    # Add Client Console Deployment
    yaml_content+="

  # -------------------------=== client-console ===------------------

  - apiVersion: apps/v1
    kind: Deployment
    metadata:
      name: network-management-console
    spec:
      selector:
        matchLabels:
          name: network-management-console
      replicas: 1
      template:
        metadata:
          labels:
            name: network-management-console
        spec:
          nodeSelector:
            kubernetes.io/hostname: client-console
          containers:
            - name: application-deployment-client
              image: murtazahr/docker-image-client:latest
              securityContext:
                privileged: true
              env:
                - name: REGISTRY_URL
                  value: \"sawtooth-registry:5000\"
                - name: VALIDATOR_URL
                  value: \"tcp://sawtooth-0:4004\"

            - name: workflow-creation-client
              image: murtazahr/workflow-creation-client:latest
              env:
                - name: VALIDATOR_URL
                  value: \"tcp://sawtooth-0:4004\"

            - name: scheduling-client
              image: murtazahr/scheduling-client:latest
              env:
                - name: VALIDATOR_URL
                  value: \"tcp://sawtooth-0:4004\""

    echo "$yaml_content"
}

# Main script starts here
echo "Enter the number of compute nodes:"
read num_compute_nodes
echo "Enter the number of IoT nodes:"
read num_iot_nodes

# Part 1: Verify inputs and check node existence
if [ "$num_compute_nodes" -lt 3 ]; then
    echo "Error: The number of compute nodes must be at least 3."
    exit 1
fi

echo "Checking for compute nodes..."
for ((i=1; i<=num_compute_nodes; i++)); do
    if ! check_node_exists "compute-node-$i"; then
        echo "Error: compute-node-$i does not exist in the cluster."
        exit 1
    fi
done

echo "Checking for IoT nodes..."
for ((i=1; i<=num_iot_nodes; i++)); do
    if ! check_node_exists "iot-node-$i"; then
        echo "Error: iot-node-$i does not exist in the cluster."
        exit 1
    fi
done

echo "All required nodes are present in the cluster."

# Part 1: Generate SSL/TLS certificates and Build sample application images
generate_ssl_certificates "$num_compute_nodes"
generate_network_key_zmq

WORK_DIR=$(pwd)
TEST_APP_DIR=$(pwd)/sample-apps

# Building docker image for test docker applications
cd "$TEST_APP_DIR/cold-chain-monitoring/task1_process_sensor_data" || exit
docker build -t process-sensor-data:latest -f Dockerfile .

cd "$TEST_APP_DIR/cold-chain-monitoring/task2_detect_anomalies" || exit
docker build -t anomaly-detection:latest -f Dockerfile .

cd "$TEST_APP_DIR/cold-chain-monitoring/task3_generate_alerts" || exit
docker build -t generate-alerts:latest -f Dockerfile .

# Make sure user is in the correct working directory
cd "$WORK_DIR" || exit

# Export test docker application
docker save -o auto-docker-deployment/docker-image-client/process-sensor-data.tar process-sensor-data
docker save -o auto-docker-deployment/docker-image-client/anomaly-detection.tar anomaly-detection
docker save -o auto-docker-deployment/docker-image-client/generate-alerts.tar generate-alerts

# Part 2: Create redis cluster
mkdir -p kubernetes-manifests/generated

/bin/bash ./redis-setup.sh "$num_compute_nodes"

# Part 3: Generate YAML file for config and secrets
# Create the PBFT key generation job YAML
cat << EOF > kubernetes-manifests/generated/pbft-key-generation-job.yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: pbft-keys
spec:
  template:
    metadata:
      labels:
        job-name: pbft-keys
    spec:
      containers:
        - name: pbft-keys-generator
          image: hyperledger/sawtooth-shell
          command:
            - bash
          args:
            - -c
            - "for i in {0..$(($num_compute_nodes-1))}; do sawadm keygen -q pbft\${i}; done && cd /etc/sawtooth/keys/ && grep '' * | sed 's/\\\\.//' | sed 's/:/:\ /'"
      restartPolicy: Never
  backoffLimit: 4
EOF

echo "Generated PBFT key generation job YAML has been saved to kubernetes-manifests/generated/pbft-key-generation-job.yaml"

# Apply the job YAML
kubectl apply -f kubernetes-manifests/generated/pbft-key-generation-job.yaml

# Wait for the job to complete
wait_for_job pbft-keys

# Get the pod name
pod_name=$(kubectl get pods --selector=job-name=pbft-keys --output=jsonpath='{.items[*].metadata.name}')

# Fetch the keys from the pod logs
generated_keys=$(kubectl logs "$pod_name")

# Delete the job YAML
kubectl delete -f kubernetes-manifests/generated/pbft-key-generation-job.yaml

echo "PBFT key generation job has been deleted."

# Process the generated keys to add proper indentation
indented_keys=$(echo "$generated_keys" | sed 's/^/      /')

# Create the config and secrets YAML
cat << EOF > kubernetes-manifests/generated/config-and-secrets.yaml
apiVersion: v1
kind: List

items:
  # --------------------------=== Blockchain Setup Keys ===----------------------
  - apiVersion: v1
    kind: ConfigMap
    metadata:
      name: keys-config
    data:
$indented_keys
  # --------------------------=== CouchDB Secrets ===---------------------------
  - apiVersion: v1
    kind: Secret
    metadata:
      name: couchdb-secrets
    type: Opaque
    stringData:
      COUCHDB_USER: trustmesh
      COUCHDB_PASSWORD: mwg478jR04vAonMu2QnFYF3sVyVKUujYrGrzVsrq3I
      COUCHDB_SECRET: LEv+K7x24ITqcAYp0R0e1GzBqiE98oSSarPD1sdeOyM=

  # --------------------------=== Docker Registry Secret ===----------------------
  - apiVersion: v1
    kind: Secret
    metadata:
      name: registry-secret
    type: Opaque
    stringData:
      http-secret: Y74bs7QpaHmI1NKDGO8I3JdquvVxL+5K15NupwxhSbc=
EOF

echo "Generated YAML file for config and secrets has been saved to kubernetes-manifests/generated/config-and-secrets.yaml"

# Part 4: Generate CouchDB cluster deployment YAML
couchdb_yaml=$(generate_couchdb_yaml "$num_compute_nodes")

# Save the generated CouchDB YAML to a file
echo "$couchdb_yaml" > kubernetes-manifests/generated/couchdb-cluster-deployment.yaml

echo "Generated CouchDB cluster deployment YAML has been saved to kubernetes-manifests/generated/couchdb-cluster-deployment.yaml"

# Part 5: Generate blockchain network deployment YAML
blockchain_network_yaml=$(generate_blockchain_network_yaml "$num_compute_nodes" "$num_iot_nodes")

# Save the generated blockchain network YAML to a file
echo "$blockchain_network_yaml" > kubernetes-manifests/generated/blockchain-network-deployment.yaml

echo "Generated blockchain network deployment YAML has been saved to kubernetes-manifests/generated/blockchain-network-deployment.yaml"

# Part 6: deploy network
echo "Deploying Network"
# Apply to kubernetes environment.
kubectl apply -f kubernetes-manifests/generated/config-and-secrets.yaml
kubectl apply -f kubernetes-manifests/generated/couchdb-cluster-deployment.yaml
kubectl apply -f kubernetes-manifests/static/local-docker-registry-deployment.yaml
kubectl apply -f kubernetes-manifests/generated/blockchain-network-deployment.yaml

echo "Script execution completed successfully."