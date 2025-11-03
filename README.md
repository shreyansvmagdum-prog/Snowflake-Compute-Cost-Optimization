# ❄️ Snowflake Compute Cost Optimization using Agentic AI

This project demonstrates how to **optimize compute costs in Snowflake** using AI-driven warehouse resizing logic implemented with **Snowpark (Python)** and **SQL**.

---

## 🎯 Objective
To reduce compute cost and improve performance by automatically resizing Snowflake virtual warehouses based on workload usage patterns.

---

## ⚙️ Architecture Overview
- **Data Source:** Snowflake `ACCOUNT_USAGE.QUERY_HISTORY`
- **AI Logic:** Agentic AI module identifies peak/off-peak hours
- **Automation:** Python stored procedures using Snowpark
- **Scheduling:** Snowflake Task triggers the process hourly

---

## 🧠 Key Components
| Step | Component | Description |
|------|------------|--------------|
| 1️⃣ | Warehouse Usage Pattern | Builds hourly workload metrics |
| 2️⃣ | FinOps Logging | Tracks warehouse resize actions & savings |
| 3️⃣ | AI Procedure | Dynamically resizes warehouses based on predicted load |
| 4️⃣ | Dashboard View | Displays monthly & cumulative cost savings |

---

## 🧰 Technologies Used
- **Snowflake Cloud Data Platform**
- **Snowpark for Python**
- **SQL**
- **Agentic AI Logic**

---

## 🧾 Code Files
| File | Description |
|------|--------------|
| `/sql/cost_optimization_script.sql` | Full implementation with DDL, Snowpark procedures, and scheduler task |
| `/assets/dashboard_screenshot.png` | (Optional) Example dashboard output |

---

## 📊 Output
- Real-time dynamic warehouse resizing
- Rolling 30-day savings view
- Estimated **30–40% cost reduction**

---

## 👤 Author
**Shreyans Magdum**  
📍 Pune, India  
🎓 B.Tech in Information Technology | G.H. Raisoni College of Engineering & Management  
📧 shreyansvmagdum@gmail.com  
🔗 [LinkedIn](https://www.linkedin.com/in/shreyans-magdum-6748b225a/)  
