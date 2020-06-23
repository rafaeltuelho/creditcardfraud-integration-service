# Credit Card Fraud Detection Integration Service

This app serves as the integration piece in the Credit Card Fraud Detection Demo.
It receives events from Kafka Streams Topic and process them using CEP Rules created with Red Hat Decision Manager.

To start this app remenber to pass the following System Properties to the JVM:

```
Dkafka.cluster.url=my-cluster-kafka-brokers:9092 -Dkafka.cluster.group.id=test -Dkie.server.url=cat-pam-kieserver:8080 -Dkie.container.id=test-case-project_1.0.0 -Dprocess.definition.id=src.fraudWorkflow -Dkie.server.auth.token=YWRtaW5Vc2VyOnRlc3QxMjM0IQ== -DrulesGroupId=com.redhat.demo.dm -DrulesArtifactId=drools-credit-card-fraud-detection -DrulesReleaseVersion=1.0.0-SNAPSHOT -DkieScannerInterval=10000
```

## When running on Openshift environment: 
 * use the following Environment Variable in the Deployment Config: `JAVA_OPTIONS`
 * use the OpenJDK 11 S2I Builder Image

This apps expects the following components running on an Openshift Cluster:

* Red Hat AMQ Streams cluster
 * Use the Strimzi Kafka Operator 
* Red Hat Process Automation (Business Central and Kie Server)
 * Use the RHPAM Operator 
* Case Management project deployed into the PAM Kie Server instance
 * https://github.com/snandakumar87/FraudCaseManagementWorkflow 
* Events Emitter (Python App) 
 * https://github.com/snandakumar87/eventEmitterCreditTransactions 
 * Use Python 3.6 S2I Builder Image 
* Kafdrop (optional)
 * use this descriptors to deploy on Openshift: https://github.com/geoallenrh/rvr/blob/master/deployment/ 