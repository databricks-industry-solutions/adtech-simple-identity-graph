# Databricks notebook source
# MAGIC %md
# MAGIC #Building a Digital Identity Graph for Ad Tech

# COMMAND ----------

# MAGIC %md This demo walks through the construction of a simple digital identity graph using advertising impression logs on Databricks. It is designed to help you understand the foundational concepts and implementation steps behind building an identity graph in a modern data and AI platform.
# MAGIC
# MAGIC
# MAGIC This README assumes you are relatively new to the Databricks Platform and our POV on Advertising Identity. If you familiar, feel free to—
# MAGIC
# MAGIC
# MAGIC Skip to: [Technical Details](#in-this-solution-accelerator)

# COMMAND ----------

# MAGIC %md
# MAGIC ## What is an Identity Graph?
# MAGIC
# MAGIC In advertising, an **Identity Graph** is typically a table or product that links multiple identity spaces together—usually at the household or individual level. These identity spaces may include:
# MAGIC
# MAGIC - Email Addresses
# MAGIC - IP Addresses
# MAGIC - Identifiers for Advertising (IFAs)
# MAGIC - First-party (1P) identifiers
# MAGIC
# MAGIC Depending on business needs, identity graphs may also incorporate traditional or **terrestrial identity spaces**—such as hashed names, physical addresses, or phone numbers. Though these are not covered in this demo, they can easily be incorporated into the workflow.

# COMMAND ----------

# MAGIC %md
# MAGIC ## So, What Does An Identity Graph Look Like?
# MAGIC A typical row in an identity graph might look like:
# MAGIC
# MAGIC
# MAGIC | household_id  | individual_id | hashed_email | primary_ifa        | primary_ip_address |
# MAGIC | --------------|---------------|--------------|--------------------|--------------------|
# MAGIC | HH001         | 123abc        | ab3...xyz    | gaid:7899-sfd-sfdf | 1.2.3.4            |
# MAGIC
# MAGIC
# MAGIC It may also include:
# MAGIC * Timestamps of last seen (e.g. from Ad Server, or Surveys/Census Data)
# MAGIC * Weights or confidence scores
# MAGIC * Multiple secondary identifiers per email or device
# MAGIC
# MAGIC
# MAGIC In this solution accelerator, we will be including timestamp and secondary identifiers to enrich our graph.
# MAGIC

# COMMAND ----------

# MAGIC %md ## What it's used for
# MAGIC
# MAGIC An identity graph is essential for advertisers who need to use anonymized audience identities at the individual or household level—based on what they know about those identities. Defining a segment by household or person is conceptually simpler and more useful for marketers, but platforms often require targeting to happen in more granular or fragmented identity spaces.
# MAGIC This is where an identity graph comes in.
# MAGIC
# MAGIC It acts as a bridge between a **unified identity** (like a household or individual) and the various identity spaces used across digital platforms (e.g., IFAs, IP addresses, emails). A well-built identity graph provides this bi-directional mapping, unlocking key capabilities:
# MAGIC
# MAGIC - **Consistent Audience Targeting**  – Accurately translate your known customer segment into the identity format required by each platform (e.g., streaming services, mobile apps, or web environments).
# MAGIC
# MAGIC - **Cross-Platform Measurement & Deduplication** – Accurately tie impressions and conversions across different devices and platforms back to the same person or household. This helps you understand true reach, de-duplicate counts, and avoid overstating your audience size.
# MAGIC
# MAGIC - **More Meaningful Attribution Studies** – Link digital ad exposures to real-world outcomes—like purchases—at the individual or household level, supporting privacy-conscious yet effective measurement and analysis.
# MAGIC
# MAGIC - **Frequency Capping** – Prevent overexposing the same person to ads across devices and platforms for a better customer experience and improved campaign efficiency.
# MAGIC
# MAGIC
# MAGIC
# MAGIC In short, a performant identity graph is foundational for modern, privacy-conscious, and effective digital advertising. Getting it right is critical.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Common Challenges
# MAGIC
# MAGIC Building and maintaining an identity graph is not without its complexities. Below are some of the most common challenges:
# MAGIC
# MAGIC #### ✅ Privacy and Consent Management
# MAGIC
# MAGIC Respecting user privacy and adhering to data regulations (like GDPR, CCPA, COPPA etc.) is foundational. Identity graphs must be designed to honor user opt-outs, manage consent signals, and avoid storing or activating data that violates user preferences.
# MAGIC
# MAGIC
# MAGIC
# MAGIC #### 🔁 Identity Refresh and Decay
# MAGIC
# MAGIC Identities change over time—people switch devices, clear cookies, or stop using certain email addresses. A performant identity graph must be able to refresh and re-evaluate links regularly to avoid using stale or misleading identity connections.
# MAGIC
# MAGIC
# MAGIC
# MAGIC #### 🔎 Data Quality
# MAGIC
# MAGIC Most digital identity data is noisy. A good identity graph pipeline must account for:
# MAGIC
# MAGIC - **Normalization**: Cleaning identifiers for consistency (e.g., stripping email addresses of tags like `+promo`, correcting common typos, or standardizing IP address formats).
# MAGIC - **Filtering**: Removing invalid records (such as fake or malformed email addresses, null values, or low-confidence identifiers).
# MAGIC - **Contextual Understanding**: Recognizing when certain identity signals might be shared or misleading (for example, hotel Wi-Fi IPs used by many users, or public/shared devices), and adjusting confidence levels accordingly.
# MAGIC
# MAGIC Without careful data curation, identity graphs can create false connections—or worse, miss real ones.
# MAGIC

# COMMAND ----------

# MAGIC %md ## Making an Identity Graph in Databricks
# MAGIC
# MAGIC <img src='./assets/img/build-buy-spectrum.png' style="object-fit:cover; object-position:50% 30%; width:300px; height:300px; zoom:1.2;"></img>
# MAGIC
# MAGIC Whether you're looking to build your own identity graph or buy a solution from a vendor or partner, Databricks can support you across the full spectrum.
# MAGIC
# MAGIC
# MAGIC If you're leaning toward the **buy** side, Databricks makes it easy to connect to pre-built identity graphs and enrichment datasets via [Databricks Marketplace](https://marketplace.databricks.com/?category=Advertising%20and%20marketing&sortBy=popularity) or partner integrations. In just a few clicks, you can hydrate your graph with high-quality data or leverage third-party solutions for identity resolution and enrichment.
# MAGIC
# MAGIC This accelerator leans toward the **build** side of the spectrum—it walks through how to construct a graph using impression log data and first-party identifiers. However, it's deliberately designed to be **modular and extensible**: you can easily enhance this example using additional identity signals, enrichment data, or **proprietary matching logic** specific to your use case. Within this accelerator we make notes on where and what can be added to make this solution your own.
# MAGIC
# MAGIC Some foundational Databricks features within this Accelerator that simplify the process and help reduce operational overhead are:
# MAGIC
# MAGIC - **Workflows** for operationalizing and automating the construction and refresh cycles of your identity graph.
# MAGIC
# MAGIC - **AI/BI Dashboards** for analytics and monitoring—offering immediate insights and supporting business applications downstream.
# MAGIC
# MAGIC - **Unity Catalog** for governing and tracking data automatically (leveraging built-in lineage).
# MAGIC
# MAGIC The diagram below shows how these tools fit together within the Databricks Platform and additional Databricks features you can leveage to accelerate your identity graph initiatives.
# MAGIC
# MAGIC <img src='./assets/img/db-identity-tools.png' style="object-fit:cover; object-position:50% 30%; width:300px; height:300px; zoom:1.2;"></img>
# MAGIC
# MAGIC
# MAGIC
# MAGIC Regardless of approach, the flexibility of the Databricks Platform helps you operationalize your identity strategy while staying in control of your data and infrastructure.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## In this solutions accelerator:
# MAGIC
# MAGIC This accelerator demonstrates how to build a simple—but fully extensible—digital identity graph on Databricks using Workflows, Unity Catalog, and an AI/BI Dashboard. You’ll start with *advertising impression logs* and progress through a [**Medallion Architecture**](https://www.databricks.com/glossary/medallion-architecture) pipeline (Bronze → Silver → Gold), resulting in a clean, query-ready *identity graph*. 
# MAGIC
# MAGIC The example is deliberately lightweight to be approachable for new users, yet modular enough for advanced teams to extend with custom logic, enrichment data, or machine learning models.
# MAGIC
# MAGIC <img src="https://www.databricks.com/sites/default/files/inline-images/building-data-pipelines-with-delta-lake-120823.png" style="object-fit:cover; object-position:50% 30%; width:300px; height:300px; zoom:1.2;"></img>
# MAGIC The Workflow orchestrates the following steps:
# MAGIC
# MAGIC
# MAGIC | Step |  Table                       | Layer   | Purpose                                        |
# MAGIC |---|----|----|----|
# MAGIC | 0    | Impression Logs              | Bronze  | Raw campaign impression activity with device-level identifiers and metadata. |
# MAGIC | 1    | Intermediate Consolidated Identity Table  | Silver  | Optimized for operational efficiency; aggregates identifiers for multiple use cases.|
# MAGIC | 2    | Individual-Level Proxy Table | Silver  |Stores identity resolution results at the individual level (not queried directly).|
# MAGIC | 2    | Household-Level Proxy Table|Silver  |Stores resolved household identifiers.|
# MAGIC | 3    | Final Identity Graph | Gold |Combines individual and household proxies into a unified, query-ready graph.|
# MAGIC
# MAGIC <img src="./assets/img/medallion-data-arch-annotated.png" style="object-fit:cover; object-position:50% 30%; width:300px; height:300px; zoom:1.2;"></img>
# MAGIC
# MAGIC This layered approach makes it easier to debug, maintain, and evolve as your matching logic becomes more sophisticated. We will go into more detail in the next notebook, the `01_Solution Launcher`.
# MAGIC
