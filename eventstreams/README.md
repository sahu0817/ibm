# IBM EventStreams — Demo Deployment on Kubernetes (kind on EC2)

This guide documents 
1. Deploy a demo IBM Event Streams instance on a **kind** cluster running on an AWS EC2 instance. It follows the official installation guide:

**Reference:** [Installing Event Streams on Kubernetes](https://ibm.github.io/event-automation/es/installing/installing-on-kubernetes/)

2. Use kcp tool to migrate to Confluent Platform
---

## Table of Contents
### Deployment

- [Prerequisites](#prerequisites)
- [1. Create the kind Cluster](#1-create-the-kind-cluster)
- [2. Add the IBM Helm Repository](#2-add-the-ibm-helm-repository)
- [3. Install Cluster-Scoped CRDs](#3-install-cluster-scoped-crds)
- [4. Install the Event Streams Operator](#4-install-the-event-streams-operator)
- [5. Create the IBM Entitlement Key Secret](#5-create-the-ibm-entitlement-key-secret)
- [6. Install NGINX Ingress Controller](#6-install-nginx-ingress-controller)
- [7. Configure and Deploy the Event Streams Instance](#7-configure-and-deploy-the-event-streams-instance)
- [8. Verify the Deployment](#8-verify-the-deployment)
- [9. Configure Local DNS (/etc/hosts)](#9-configure-local-dns-etchosts)
- [10. Create the Admin User](#10-create-the-admin-user)
- [11. Access the Admin UI](#11-access-the-admin-ui)
- [12. Install the Event Streams CLI](#12-install-the-event-streams-cli)
- [13. Manage Kafka Topics](#13-manage-kafka-topics)
- [14. Manage Kafka Schemas](#14-manage-kafka-schemas)
- [15. Run the Starter Application](#15-run-the-starter-application)
- [16. Produce with Schema using REST API](#16-produce-with-schema-using-rest-api)

### kcp 
- [Prerequisites](#kcp-prerequisites)
- [1. Scan the eventstreams cluster](#1-scan-the-eventstreams-cluster)
---

## Prerequisites

| Requirement | Notes |
|---|---|
| **kind** | Kubernetes in Docker |
| **kubectl** | Configured for the kind cluster |
| **Helm 3** | Package manager for Kubernetes |
| **IBM Entitlement Key** | Obtain from [IBM Container Library](https://myibm.ibm.com/products-services/containerlibrary) |
| **EC2 instance** | With ports **80**, **443**, and **9094** open in the security group |
| **Java 11+** | Required for the starter application demo |

> **Note:** This deployment uses a **kind** cluster on EC2. The custom `kind-config.yaml` in this directory binds ports 80, 443, and 9094 from the EC2 host directly into the kind control-plane container.

> **Warning:** If Apache2 is listening on port 80, stop it before creating the cluster:
>
> ```bash
> sudo systemctl stop apache2
> ```

> **Important:** Use **multiple worker nodes** in the kind cluster to support installation profiles like `minimal-prod` with multiple Kafka brokers. The NGINX ingress controller must be **pinned to the control-plane node** because host-port bindings are on the control-plane only.

---

## 1. Create the kind Cluster

Use the provided `kind-config.yaml`, which defines one control-plane node (with host port mappings) and two worker nodes:

```bash
cd ~/ibm/eventstreams
kind create cluster -n confluent --config kind-config.yaml
```

**Output:**

```
Creating cluster "confluent" ...
 ✓ Ensuring node image (kindest/node:v1.27.1) 🖼
 ✓ Preparing nodes 📦 📦 📦
 ✓ Writing configuration 📜
 ✓ Starting control-plane 🕹️
 ✓ Installing CNI 🔌
 ✓ Installing StorageClass 💾
 ✓ Joining worker nodes 🚜
Set kubectl context to "kind-confluent"
You can now use your cluster with:

kubectl cluster-info --context kind-confluent
```

Verify the cluster nodes and port bindings:

```bash
docker ps
```

**Output:**

```
CONTAINER ID   IMAGE                  COMMAND                  CREATED              STATUS              PORTS                                                                                         NAMES
f8200ade7862   kindest/node:v1.27.1   "/usr/local/bin/entr…"   About a minute ago   Up About a minute   0.0.0.0:80->80/tcp, 0.0.0.0:443->443/tcp, 0.0.0.0:9094->9094/tcp, 127.0.0.1:39089->6443/tcp   confluent-control-plane
a633e7f26f10   kindest/node:v1.27.1   "/usr/local/bin/entr…"   About a minute ago   Up About a minute                                                                                                 confluent-worker2
e0efd40eda9f   kindest/node:v1.27.1   "/usr/local/bin/entr…"   About a minute ago   Up About a minute                                                                                                 confluent-worker
```

---

## 2. Add the IBM Helm Repository

```bash
helm repo add ibm-helm https://raw.githubusercontent.com/IBM/charts/master/repo/ibm-helm
```

---

## 3. Install Cluster-Scoped CRDs

Install cluster-scoped Custom Resource Definitions (CRDs) first. Example: `crd-release-name=es-crds`, `namespace=confluent`.

```bash
helm install es-crds ibm-helm/ibm-eventstreams-operator \
  -n confluent \
  --set namespaceScopedResources=false \
  --create-namespace
```

**Output:**

```
NAME: es-crds
LAST DEPLOYED: Sat Jun 13 05:13:57 2026
NAMESPACE: confluent
STATUS: deployed
REVISION: 1
TEST SUITE: None
NOTES:
====
    IBM Confidential
    © Copyright IBM Corp. 2020, 2024
====

ibm-eventstreams-operator version 12.3.1 - CRDs installed.

Your release is named es-crds.

Custom Resource Definitions (CRDs) have been installed.

Next step: Install the Event Streams operator in your target namespace(s) using:
  helm install <release-name> <chart> -n <namespace> --set clusterScopedResources=false
```

---

## 4. Install the Event Streams Operator

```bash
helm install eventstreams ibm-helm/ibm-eventstreams-operator \
  -n my-eventstreams \
  --set clusterScopedResources=false \
  --set watchAnyNamespace=true \
  --create-namespace
```

**Output:**

```
NAME: eventstreams
LAST DEPLOYED: Sat Jun 13 05:15:41 2026
NAMESPACE: my-eventstreams
STATUS: deployed
REVISION: 1
TEST SUITE: None
NOTES:
====
    IBM Confidential
    © Copyright IBM Corp. 2020, 2024
====

ibm-eventstreams-operator version 12.3.1 installed.

Your release is named eventstreams.

The Event Streams operator has been installed in namespace my-eventstreams.
Note: CRDs were skipped. Ensure CRDs are installed separately before creating Event Streams instances.
```

Verify the operator is running (typically ready in under 1 minute):

```bash
kubectl get deploy eventstreams-cluster-operator -n my-eventstreams
```

**Output:**

```
NAME                            READY   UP-TO-DATE   AVAILABLE   AGE
eventstreams-cluster-operator   1/1     1            1           3m20s
```

---

## 5. Create the IBM Entitlement Key Secret

Obtain your entitlement key from the [IBM Container Library](https://myibm.ibm.com/products-services/containerlibrary), then create a pull secret:

```bash
kubectl create secret docker-registry ibm-entitlement-key \
  --docker-username=cp \
  --docker-password="<YOUR_IBM_ENTITLEMENT_KEY>" \
  --docker-server="cp.icr.io" \
  -n my-eventstreams
```

**Output:**

```
secret/ibm-entitlement-key created
```

> **Warning:** Never commit your entitlement key to source control. Store it in a secrets manager or pass it via environment variable at deploy time.

---

## 6. Install NGINX Ingress Controller

Because this deployment runs on kind inside a remote EC2 instance, you must:

1. Enable **SSL passthrough** (required by IBM Event Streams).
2. Configure **host ports** so EC2 can route HTTP/HTTPS traffic into the Docker containers.
3. Pin the ingress controller to the **control-plane** node.

Replace `<EC2_PUBLIC_IP>` with your instance's public IP (e.g. `99.999.99.999`).

```bash
helm repo add ingress-nginx https://kubernetes.github.io/ingress-nginx
helm repo update
```

Label the control-plane node so the ingress controller schedules there:

```bash
kubectl label node confluent-control-plane ingress-ready=true
```

Install ingress-nginx:

```bash
helm install ingress-nginx ingress-nginx/ingress-nginx \
  --namespace ingress-nginx \
  --create-namespace \
  --set controller.kind=Deployment \
  --set controller.replicaCount=1 \
  --set controller.hostPort.enabled=true \
  --set controller.service.type=NodePort \
  --set controller.ingressClassResource.name=nginx \
  --set controller.ingressClass=nginx \
  --set controller.extraArgs.enable-ssl-passthrough=true \
  --set controller.publishService.enabled=false \
  --set-string controller.extraArgs.publish-status-address=<EC2_PUBLIC_IP> \
  --set-string controller.nodeSelector.ingress-ready=true \
  --set controller.tolerations[0].key=node-role.kubernetes.io/control-plane \
  --set controller.tolerations[0].operator=Exists \
  --set controller.tolerations[0].effect=NoSchedule
```

**Output:**

```
NAME: ingress-nginx
LAST DEPLOYED: Wed Jun 17 21:52:11 2026
NAMESPACE: ingress-nginx
STATUS: deployed
REVISION: 1
TEST SUITE: None
NOTES:
The ingress-nginx controller has been installed.
```

Confirm the controller is on the control-plane node (not a worker):

```bash
kubectl get pods -n ingress-nginx -o wide

NAME                                        READY   STATUS    RESTARTS   AGE    IP           NODE                      NOMINATED NODE   READINESS GATES
ingress-nginx-controller-75bbf79b77-rrzwv   1/1     Running   0          8m7s   10.244.0.5   confluent-control-plane   <none>           <none>
```

Note the ingress class and storage class names — you will need them when editing `minimal-prod.yaml`:

```bash
kubectl get ingressclass

NAME    CONTROLLER             PARAMETERS   AGE
nginx   k8s.io/ingress-nginx   <none>       107s
```

```bash
kubectl get storageclass

NAME                 PROVISIONER             RECLAIMPOLICY   VOLUMEBINDINGMODE      ALLOWVOLUMEEXPANSION   AGE
standard (default)   rancher.io/local-path   Delete          WaitForFirstConsumer   false                  10m
```

> **Note:** Use ingress class `nginx` and storage class `standard` in the Event Streams custom resource.

---

## 7. Configure and Deploy the Event Streams Instance

Download the CR from ibm-event-automation [git repo](https://github.com/IBM/ibm-event-automation/tree/main/event-streams/cr-examples/eventstreams/kubernetes).

Edit `minimal-prod.yaml` before applying:

1. Set `spec.license.accept` to `true`.
2. Replace `<HOSTNAME>` placeholders with sslip.io hostnames based on your EC2 public IP.
   - Example: for IP `99.999.99.999`, use `adminui.99-999-99-999.sslip.io`.
3. Set `class: nginx` for all ingress endpoints.
4. Set `class: standard` for storage.

> **Note:** FQDNs ending with sslip.io are magic DNS records that resolve directly to the IP address embedded inside the domain name itself. For example, 192.168.1.100.sslip.io automatically resolves to 192.168.1.100. If you choose to go with a custom hostname you need to provision DNS records

Example diff (original vs. configured):

```bash
sdiff -s minimal-prod.yaml.orig minimal-prod.yaml
```

**Output:**

```
    accept: false                                    |      accept: true
        host: <HOSTNAME>                             |          host: adminapi.<ip-with-dashes>.sslip.io
        class: <INGRESS-CLASS>                       |          class: nginx
        host: <HOSTNAME>                             |          host: adminui.<ip-with-dashes>.sslip.io
        class: <INGRESS-CLASS>                       |          class: nginx
        host: <HOSTNAME>                             |          host: apicurio.<ip-with-dashes>.sslip.io
        class: <INGRESS-CLASS>                       |          class: nginx
        host: <HOSTNAME>                             |          host: rest.<ip-with-dashes>.sslip.io
        class: <INGRESS-CLASS>				               |          class: nginx
              host: <HOSTNAME>                       |                host: kafka.<ip-with-dashes>.sslip.io
            hostTemplate: broker-{nodeId}.<HOSTNAME> |              hostTemplate: broker-{nodeId}.<ip-with-dashes>.sslip.io
            class: <INGRESS-CLASS>                   |              class: nginx
          class: ''                                  |            class: standard
          class: ''                                  |            class: standard
```

Deploy the instance:

```bash
kubectl apply -f minimal-prod.yaml -n my-eventstreams

eventstreams.eventstreams.ibm.com/minimal-prod created
```
---

## 8. Verify the Deployment

The instance typically reaches `Ready` status in under 5 minutes.

```bash
kubectl get eventstreams -n my-eventstreams

NAME           STATUS
minimal-prod   Ready
```

```bash
kubectl get all -n my-eventstreams

NAME                                                 READY   STATUS    RESTARTS        AGE
pod/eventstreams-cluster-operator-6f484c4bf7-z7nf5   1/1     Running   0               18m
pod/minimal-prod-controller-0                        1/1     Running   0               6m35s
pod/minimal-prod-controller-1                        1/1     Running   0               6m35s
pod/minimal-prod-controller-2                        1/1     Running   0               6m35s
pod/minimal-prod-entity-operator-844f775c7f-hvhv7    2/2     Running   0               4m25s
pod/minimal-prod-ibm-es-ac-reg-64fdf49545-lf6tg      2/2     Running   1 (105s ago)    3m58s
pod/minimal-prod-ibm-es-admapi-867c49ccf-29l7t       1/1     Running   0               3m56s
pod/minimal-prod-ibm-es-recapi-564b76c44c-xccms      1/1     Running   0               3m57s
pod/minimal-prod-ibm-es-ui-6bd7fc9995-krjmj          2/2     Running   0               3m56s
pod/minimal-prod-kafka-3                             1/1     Running   1 (4m55s ago)   6m35s
pod/minimal-prod-kafka-4                             1/1     Running   0               6m35s
pod/minimal-prod-kafka-5                             1/1     Running   1 (4m55s ago)   6m35s

NAME                                            TYPE        CLUSTER-IP      EXTERNAL-IP   PORT(S)     AGE
service/eventstreams-cluster-operator           ClusterIP   10.96.63.184    <none>        443/TCP     18m
service/minimal-prod-ibm-es-ac-reg-ingress      ClusterIP   10.96.189.247   <none>        8002/TCP    3m58s
service/minimal-prod-ibm-es-ac-reg-internal     ClusterIP   10.96.235.235   <none>        7443/TCP    3m58s
service/minimal-prod-ibm-es-admapi-ingress      ClusterIP   10.96.227.24    <none>        8001/TCP    3m57s
service/minimal-prod-ibm-es-admapi-internal     ClusterIP   10.96.15.108    <none>        7443/TCP    3m57s
service/minimal-prod-ibm-es-recapi-ingress      ClusterIP   10.96.195.10    <none>        8003/TCP    3m58s
service/minimal-prod-ibm-es-recapi-internal     ClusterIP   10.96.171.39    <none>        7443/TCP    3m57s
service/minimal-prod-ibm-es-ui-ingress          ClusterIP   10.96.191.223   <none>        3000/TCP    3m56s
service/minimal-prod-kafka-3                    ClusterIP   10.96.148.29    <none>        9094/TCP    7m21s
service/minimal-prod-kafka-4                    ClusterIP   10.96.186.185   <none>        9094/TCP    7m21s
service/minimal-prod-kafka-5                    ClusterIP   10.96.215.110   <none>        9094/TCP    7m21s
service/minimal-prod-kafka-bootstrap            ClusterIP   10.96.190.132   <none>        9091/TCP,9093/TCP                              7m21s
service/minimal-prod-kafka-brokers              ClusterIP   None            <none>        9090/TCP,9091/TCP,8443/TCP,9093/TCP,9404/TCP   7m21s
service/minimal-prod-kafka-external-bootstrap   ClusterIP   10.96.50.109    <none>        9094/TCP    7m21s

NAME                                            READY   UP-TO-DATE   AVAILABLE   AGE
deployment.apps/eventstreams-cluster-operator   1/1     1            1           18m
deployment.apps/minimal-prod-entity-operator    1/1     1            1           4m25s
deployment.apps/minimal-prod-ibm-es-ac-reg      1/1     1            1           3m58s
deployment.apps/minimal-prod-ibm-es-admapi      1/1     1            1           3m56s
deployment.apps/minimal-prod-ibm-es-recapi      1/1     1            1           3m57s
deployment.apps/minimal-prod-ibm-es-ui          1/1     1            1           3m56s

NAME                                                       DESIRED   CURRENT   READY   AGE
replicaset.apps/eventstreams-cluster-operator-6f484c4bf7   1         1         1       18m
replicaset.apps/minimal-prod-entity-operator-844f775c7f    1         1         1       4m25s
replicaset.apps/minimal-prod-ibm-es-ac-reg-64fdf49545      1         1         1       3m58s
replicaset.apps/minimal-prod-ibm-es-admapi-867c49ccf       1         1         1       3m56s
replicaset.apps/minimal-prod-ibm-es-recapi-564b76c44c      1         1         1       3m57s
replicaset.apps/minimal-prod-ibm-es-ui-6bd7fc9995          1         1         1       3m56s

NAME                                             STATUS
eventstreams.eventstreams.ibm.com/minimal-prod   Ready
```

```bash
kubectl get ingress -n my-eventstreams

NAME                                 CLASS   HOSTS                                ADDRESS           PORTS     AGE
minimal-prod-ibm-es-ac-reg-ingress   nginx   apicurio.<ip-with-dashes>.sslip.io   <EC2.PUBLIC.ip>   80, 443   4m36s
minimal-prod-ibm-es-admapi-ingress   nginx   adminapi.<ip-with-dashes>.sslip.io   <EC2.PUBLIC.ip>   80, 443   4m35s
minimal-prod-ibm-es-recapi-ingress   nginx   rest.<ip-with-dashes>.sslip.io       <EC2.PUBLIC.ip>   80, 443   4m35s
minimal-prod-ibm-es-ui-ingress       nginx   adminui.<ip-with-dashes>.sslip.io    <EC2.PUBLIC.ip>   80, 443   4m34s
minimal-prod-kafka-3                 nginx   broker-3.<ip-with-dashes>.sslip.io   <EC2.PUBLIC.ip>   80, 443   7m58s
minimal-prod-kafka-4                 nginx   broker-4.<ip-with-dashes>.sslip.io   <EC2.PUBLIC.ip>   80, 443   7m58s
minimal-prod-kafka-5                 nginx   broker-5.<ip-with-dashes>.sslip.io   <EC2.PUBLIC.ip>   80, 443   7m58s
minimal-prod-kafka-bootstrap         nginx   kafka.<ip-with-dashes>.sslip.io      <EC2.PUBLIC.ip>   80, 443   7m58s
```

---

## 9. Configure Local DNS (/etc/hosts)

On your **local machine** (not the EC2 host), add entries so sslip.io hostnames resolve to the EC2 public IP:

```bash
# /etc/hosts — Event Streams on EC2
<EC2_PUBLIC_IP> apicurio.<ip-with-dashes>.sslip.io
<EC2_PUBLIC_IP> adminapi.<ip-with-dashes>.sslip.io
<EC2_PUBLIC_IP> adminui.<ip-with-dashes>.sslip.io
<EC2_PUBLIC_IP> rest.<ip-with-dashes>.sslip.io
<EC2_PUBLIC_IP> broker-3.<ip-with-dashes>.sslip.io
<EC2_PUBLIC_IP> broker-4.<ip-with-dashes>.sslip.io
<EC2_PUBLIC_IP> broker-5.<ip-with-dashes>.sslip.io
<EC2_PUBLIC_IP> kafka.<ip-with-dashes>.sslip.io
```

> **Note:** sslip.io automatically resolves `*.<ip-with-dashes>.sslip.io` to the dotted IP, but adding `/etc/hosts` entries ensures reliable resolution from your workstation.

---

## 10. Create the Admin User

Create the admin user **before** logging into the Admin UI:

```bash
kubectl apply -f kafkauser.yaml -n my-eventstreams

Warning: Version v1beta2 of the KafkaUser API is deprecated. Please use the v1 version instead.
kafkauser.eventstreams.ibm.com/admin created
```

Retrieve the username and secret name:

```bash
kubectl get kafkauser admin -n my-eventstreams \
  -o jsonpath='{"username: "}{.status.username}{"\nsecret: "}{.status.secret}{"\n"}'

username: admin
secret: admin
```

Retrieve the password:

```bash
kubectl get secret admin -n my-eventstreams \
  -o jsonpath='{.data.password}' | base64 -d; echo

<ADMIN_PASSWORD>
```

> **Warning:** The password is generated at creation time. Save it securely; you will need it for the Admin UI and CLI login.

---

## 11. Access the Admin UI

Open in your browser:

```
https://adminui.<ip-with-dashes>.sslip.io/
```

Log in with:

- **Username:** `admin`
- **Password:** value retrieved from the secret above

![Event Streams Admin UI](images/eventstreams_ui.png)

---

## 12. Install the Event Streams CLI

The CLI supports:

- Creating, deleting, and updating Kafka topics
- Managing Kafka message schemas
- Managing geo-replication
- Displaying cluster configuration and credentials

**Download:** In the Admin UI, go to **Toolbox → Command-line Interface** and download `kubectl-es-plugin.bin`.

![Event Streams CLI download](images/eventstreams_cli_download.png)

Install on the EC2 host:

```bash
chmod 755 kubectl-es-plugin.bin
sudo mv kubectl-es-plugin.bin /usr/local/bin/kubectl-es
```

Verify:

```bash
kubectl es
```

**Output: (abbreviated)**

```
NAME:
   kubectl event-streams - Manage IBM Event Streams

USAGE:
   kubectl es [global options] command [command options] [arguments...]

VERSION:
   12.3.1 (2604270910)

COMMANDS:
   help, h  Shows a list of commands or help for one command
   . . .
```

Initialize the CLI session:

```bash
kubectl es init -n my-eventstreams \
  --auth-type scram-sha-512 \
  --username admin \
  --password <ADMIN_PASSWORD> \
  --schema-reg-url https://apicurio.<ip-with-dashes>.sslip.io

Namespace:                         my-eventstreams
Name:                              minimal-prod
Event Streams API endpoint:        https://adminapi.<ip-with-dashes>.sslip.io
Event Streams API status:          OK
Event Streams UI address:          https://adminui.<ip-with-dashes>.sslip.io
Apicurio Registry endpoint:        https://apicurio.<ip-with-dashes>.sslip.io
Event Streams bootstrap address:   kafka.<ip-with-dashes>.sslip.io:443
OK
```

---

## 13. Manage Kafka Topics

### Create a topic via CLI

```bash
kubectl es topic-create \
  --name my-topic \
  --partitions 1 \
  --replication-factor 1 \
  --config retention.ms=86400000

Created topic my-topic
OK
```

### Create a topic via YAML

```bash
kubectl apply -f kafkatopic.yaml -n my-eventstreams

Warning: Version v1beta2 of the KafkaTopic API is deprecated. Please use the v1 version instead.
kafkatopic.eventstreams.ibm.com/my-yaml-topic created
```

### Delete a topic via CLI

```bash
kubectl es topic-delete --name my-yaml-topic

Really delete topic 'my-yaml-topic'? [y/N]> y
Topic my-yaml-topic deleted successfully
OK
```

> **Practical rule:**
>
> | Created with | Delete with |
> |---|---|
> | `kubectl es topic-create` | `kubectl es topic-delete` |
> | `kubectl apply -f kafkatopic.yaml` | `kubectl delete kafkatopic <name> -n my-eventstreams` |

---
## 14. Manage Kafka Schemas

### Register a Schema via CLI

PreReq: The user needs to have these ACLs to register schema
```bash
- operation: Read
  resource:
    type: topic
    name: __schema_
    patternType: prefix
- operation: Alter
  resource:
    type: topic
    name: __schema_
    patternType: prefix

```
You can either do a quick inline edit
```bash
kubectl edit kafkauser admin -n my-eventstreams
```
OR the safest way
```bash
kubectl get kafkauser admin -n my-eventstreams -o yaml > admin-user.yaml
<edit the file to add ACLs>
kubectl apply -f admin-user.yaml
```

```bash
kubectl es schema-add --name test-value --version v1 --file test.avsc

The latest version of schema test-value is enabled.

Version   Version ID   Schema       State     Updated                Comment
v1        1            test-value   enabled   2026-06-18T21:17:07Z

Added version v1 of schema test-value to the registry.
OK
```

---

## 15. Run the Starter Application

Download the demo JAR and configure it from the Admin UI. 

Use this UI option for  latest jar and the manual step for a specific version.

**Option A:** Admin UI → **Toolbox → Starter application**

**Option B:** Manual setup:

```bash
mkdir app && cd app
wget http://github.com/ibm-messaging/kafka-java-vertx-starter/releases/download/1.1.5/demo-all.jar
```

In the Admin UI (**Toolbox → Starter application → Configure & run starter application**):

| Setting | Value |
|---|---|
| App name | `starter-app` |
| Topic name | `starter_topic` |

Download `starter-app_properties.zip`, then:

```bash
unzip starter-app_properties.zip
ls -ltr
```

**Output:**

```
-rw-rw-r-- 1 ubuntu ubuntu     1702 Jun 18 01:48 truststore.p12
-rw-rw-r-- 1 ubuntu ubuntu      859 Jun 18 01:48 kafka.properties
-rw-r--r-- 1 ubuntu ubuntu     2387 Jun 18 01:49 starter-app_properties.zip
-rw-rw-r-- 1 ubuntu ubuntu 38510290 Jun 18 01:49 demo-all.jar
```
> **Note:** The kafka.properties & truststore.p12 is dynamically created by the UI Starter Application. The files in this repo is for reference only.

Run the application:

```bash
java -Dproperties_path=./kafka.properties -jar demo-all.jar

2026-06-18 01:55:25,459 INFO [vert.x-eventloop-thread-0] kafka.vertx.demo.Main - Application version: 1.1.5
2026-06-18 01:55:25,908 INFO [vert.x-eventloop-thread-2] kafka.vertx.demo.WebSocketServer - 🚀 WebSocketServer started
2026-06-18 01:55:26,432 INFO [vert.x-eventloop-thread-1] kafka.vertx.demo.PeriodicProducer - 🚀 PeriodicProducer started
2026-06-18 01:55:26,433 INFO [vert.x-eventloop-thread-0] kafka.vertx.demo.Main - ✅ Application started in 1357ms
2026-06-18 01:56:15,590 INFO [vert.x-eventloop-thread-2] kafka.vertx.demo.WebSocketServer - Subscribed to starter_topic
2026-06-18 01:58:59,308 INFO [vert.x-eventloop-thread-1] kafka.vertx.demo.PeriodicProducer - Producing Kafka records with message template: {   {"name": "Jane Doe",   "empid": "EMP-84920",   "zipcode": "75070" }
2026-06-18 01:59:13,764 INFO [vert.x-eventloop-thread-1] kafka.vertx.demo.PeriodicProducer - Stopped producing Kafka records
```

Access the Starter App UI to produce & consume events

Open in your browser:

```
http://<EC2_PUBLIC_IP>:8080/
```

![Starter application UI](images/starterapp_ui.png)

---
## 16. Produce with Schema using REST API

#### Create Scram Credentials
On the eventstreams UI: Go to **Home → Connect to this Cluster → Producer endpoint and credentials → Generate Credentials → SCRAM username and password**

user : restapi

password: blablablabla

Basic AuthToken: Basic abcdefghijklmnopqrstuvwxyz==

#### Download the server certificate for EventStreams
```bash
kubectl es certificates --format pem

Certificate successfully written to /home/ubuntu/ibm/event-automation/es-cert.pem.
OK
```

#### Create a Ropic
```bash
kubectl es topic-create --name restapi-topic --partitions 1 --replication-factor 1 --config retention.ms=86400000

Created topic restapi-topic
OK
```

#### Create a Schema
```bash
kubectl es schema-add --name restapi-schema --version v1 --file restapi.avsc

The latest version of schema restapi-schema is enabled.
Version   Version ID   Schema           State     Updated                Comment
v1        1            restapi-schema   enabled   2026-06-19T03:49:08Z

Added version v1 of schema restapi-schema to the registry.
OK
```

#### Produce with Schema
```bash
curl -H "Authorization: Basic abcdefghijklmnopqrstuvwxyz==" -H "Accept: application/json" -H "Content-Type: text/plain" -d '{"name": "John", "number" : 2}' --cacert es-cert.pem "https://rest.18-220-31-188.sslip.io/topics/restapi-topic/records?schemaname=restapi-schema&schemaversion=v1"

{
  "metadata" : {
    "topic" : "restapi-topic",
    "partition" : 0,
    "offset" : 1,
    "timestamp" : 1781841235780
  },
  "topic" : "restapi-topic",
  "partition" : 0,
  "offset" : 1,
  "timestamp" : 1781841235780
```
---
## KCP Prerequisites

| Requirement | Notes |
|---|---|
| **kcp** | Download the latest from [here](https://github.com/confluentinc/kcp/releases|)

```bash
wget https://github.com/confluentinc/kcp/releases/download/v0.8.5/kcp_linux_amd64.tar.gz
tar -xzf kcp_linux_amd64.tar.gz
sudo cp kcp/kcp /usr/local/bin
```
---

## 1. Scan the eventstreams cluster 
```bash
kcp scan clusters --source-type apache-kafka --credentials-file apache-kafka-credentials.yaml

ℹ️  Apache Kafka credentials file format & metrics options: https://confluentinc.github.io/kcp/0.8.5/apache-kafka-configuration/
2026/06/22 16:19:28 WARN TLS certificate verification is disabled - this should only be used in test environments with self-signed certificates

✅ Scan completed successfully
   Scanned 1 cluster(s)
   State file: kcp-state.json
```
---

## Additional Resources

- [Event Streams Documentation](https://ibm.github.io/event-automation/es/)
- [CR Examples (GitHub)](https://github.com/IBM/ibm-event-automation/tree/main/event-streams/cr-examples/eventstreams/kubernetes)
- [IBM Container Library (Entitlement Key)](https://myibm.ibm.com/products-services/containerlibrary)
- [KCP Migration Tool](https://github.com/confluentinc/kcp/tree/main)
