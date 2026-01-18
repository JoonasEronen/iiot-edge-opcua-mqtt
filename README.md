# IIoT Edge Component: OPC UA → MQTT (AWS Greengrass)

Industrial IoT edge component developed as part of a bachelor’s thesis.  

The component reads process data from an OPC UA server and publishes structured MQTT messages using a Unified Namespace (UNS)–style topic hierarchy. 

Designed to run as an AWS Greengrass V2 Java component on an edge gateway.

---

## What this repository is

- Public **reference implementation** of an industrial IIoT edge component
- Focuses on **edge architecture**, protocol integration, and configuration patterns
- Intended for **learning, demonstration, and concept validation**
- **Cloud-side implementations are not included** (see Cloud overview)

This repository represents the author’s own implementation developed during the thesis.

---

## Key features

- OPC UA client implementation using **Eclipse Milo**
- MQTT publishing using **Eclipse Paho**
- Designed for **AWS Greengrass V2** (Java component model)
- UNS-style hierarchical MQTT topic structure
- Configuration via local resources and Greengrass configuration patterns
- Designed for continuous operation on a constrained edge gateway

---

## High-level edge architecture

- Reads simulated or live process data from an OPC UA server
- Performs basic validation and structuring at the edge
- Publishes MQTT messages using a UNS-inspired hierarchical topic model
- Operates as a long-running edge component under AWS Greengrass

This repository intentionally focuses on **edge responsibilities**;  
cloud services are treated as managed infrastructure and are documented at a conceptual level in the thesis.

---

## Cloud overview (high level)

In the thesis project, the edge component publishes MQTT messages to AWS IoT Core
and was validated as part of a broader edge–cloud architecture.

Cloud-side processing in the project utilized managed AWS services such as:

- AWS IoT Core (rules-based routing)
- AWS Lambda
- Amazon DynamoDB
- Amazon Timestream
- AWS IoT SiteWise
- Visualization tools including Grafana and QuickSight

This public repository focuses exclusively on the **edge component**
and does not include environment-specific cloud infrastructure, automation, or deployment code.

---

## Background (Bachelor’s thesis)

This edge component was developed as part of a bachelor’s thesis focusing on:

- Cost-effective industrial edge–cloud integration
- Unified Namespace (UNS) principles
- Resource- and cost-aware IIoT architectures
- Secure OPC UA and MQTT-based communication

> Thesis (PDF): https://urn.fi/URN:NBN:fi:amk-2025112830694

---

## Tech stack

- Java 21  
- AWS Greengrass V2  
- Eclipse Milo (OPC UA)  
- Eclipse Paho (MQTT)  
- AWS SDK for Java  

---

## Security & OPSEC

All examples use generic identifiers and placeholder values.  
No credentials, certificates, device identifiers, or customer-specific configuration are included.

---

## Disclaimer

This project is provided as a **reference implementation and concept-level example**.  
It is not intended for direct production use without review, adaptation, testing, and security hardening.

---

## License

MIT License
