**"Data-Analytics-and-Engineering\_Fullstack"** project:

---

# 🎯 Data Analytics and Engineering — Full Stack Project

### 📊 End-to-End Data Engineering & Reporting Solution

A comprehensive project covering ETL, Data Modelling, and Reporting using **SSIS**, **SSRS**, and **Power BI**.

---

## 📁 Project Structure

```
📁 Data-Analytics-and-Engineering_Fullstack/
├── SSIS-ETL/ ➡ ETL packages built with SSIS (Data Extraction, Transformation, Loading)
├── Source-University Data ➡ Flat Files, Excel, CSV source data
├── SSRS-ETL & Reporting ➡ SSRS Reports (Faculty Ratio, Nationality Summary, Financial Reports)
├── Power BI ➡ Power BI dashboards and reports
├── Other Power BI Projects ➡ Sample projects and practice reports
├── Power BI Reference ➡ Self-practice tutorials
├── PySpark/ ➡ Big Data Processing using PySpark on Databricks
```

---

## 🛠️ Technologies Used

* **Microsoft SQL Server Integration Services (SSIS)** — ETL Pipeline
* **Microsoft SQL Server Reporting Services (SSRS)** — Paginated Reports
* **Power BI** — Data Visualization & Interactive Reports
* * **Apache PySpark (Databrick)** — Distributed Data Processing & Transformation

---

## 🔥 Project Highlights

### 1️⃣ **ETL with SSIS**

* Loaded data from multiple sources (Excel, CSV)
* Data cleansing, transformation, and loading into SQL Server
* Combined Teaching & Non-Teaching Faculty with derived columns

🖼️

<img width="524" height="493" alt="ControlFlow" src="https://github.com/user-attachments/assets/1f6bb5aa-17ad-47ea-9191-9499cbe428df" />
<img width="1854" height="798" alt="DataFlow" src="https://github.com/user-attachments/assets/6b36b431-c0fb-4c93-9a1b-b1a8fd110065" />


---

### 2️⃣ **Database Structure in SQL Server**

* Centralized University Data Warehouse
* Connected Fact & Dimension Tables

🖼️
<img width="1938" height="1378" alt="initialtop3DB" src="https://github.com/user-attachments/assets/82ec184a-670c-4478-82e4-75574a496b36" />


---

### 3️⃣ **SSRS Reports**

* **Faculty to Student Ratio Report**
* **Nationality Summary Report**
* **Faculty Course Summary Report**

🖼️
<img width="859" height="1321" alt="SSRS- Faculty Ratio Report" src="https://github.com/user-attachments/assets/e8bc0567-0111-4e87-801c-f52c2af372b5" />
<img width="555" height="1315" alt="FacutyCourseSumarry" src="https://github.com/user-attachments/assets/85a18165-55c4-457c-b13f-4c560fb2331d" />
<img width="885" height="939" alt="SSRS nationalitysummary (3)" src="https://github.com/user-attachments/assets/baed483b-71b9-465d-aec4-a7f5600efc61" />


---

### 4️⃣ **Power BI Dashboards**

* Faculty Nationality Summary with Map Visuals
* Student-to-Faculty Ratio Reports
* Department-Wise Student and Faculty Analysis

🖼️
Data Model/ Schema: <img width="1202" height="756" alt="Data Model- Schema" src="https://github.com/user-attachments/assets/03d4e54b-d16e-40fe-9421-e4bc6d98edd3" />
1/3 Report/Dashboard: <img width="1930" height="1098" alt="image" src="https://github.com/user-attachments/assets/d828c8b7-cd41-4516-bced-1dd829f102cd" />


---

## 📝 How to Run the Project

1️⃣ **ETL**

* Open `.dtsx` packages in Visual Studio
* Run Control & Data Flow Tasks

2️⃣ **Database**

* Restore `DataAnalytics_ETL_Dashboard.bak` in SQL Server
* Check tables under `DataAnalytics_ETL_Dashboard`

3️⃣ **SSRS Reports**

* Open `.rdl` files in Visual Studio
* Deploy to Report Server or Preview Locally

4️⃣ **Power BI**

* Open `.pbix` files in Power BI Desktop
* Refresh data to see live results

5️⃣  Big Data Processing with PySpark (Databricks)

PySpark notebooks created and executed on Databricks

Large datasets processed using distributed Spark DataFrames

Applied transformations such as filtering, joins, aggregations, and derived metrics

Prepared curated datasets for downstream analytics and reporting

---

## 🎯 Outcome

✔️ Automated ETL Pipeline
✔️ Centralized Reporting with SSRS
✔️ Interactive Dashboards with Power BI
✔️ Realistic University Analytics Scenario

---

## ✨ Author

**Zahid — Data Analytics Enthusiast**
📫 Feel free to connect or fork this project!

---

## 📌 Note

> This project is for educational purposes and a demonstration of full-stack data analytics with Microsoft Tools.

---
