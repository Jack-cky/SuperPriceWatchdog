<!-- INTRODUCTION -->
# 🐶 SuperPriceWatchdog
<div align="center">
  <a href="https://online-price-watch.consumer.org.hk/opw/"><img src="./imgs/banner.png"></a>
  <a href="https://t.me/SuperPriceWatchdogBot"><img src="https://img.shields.io/badge/Telegram-2CA5E0?&logo=Telegram&logoColor=white"></a>
  <a href="https://online-price-watch.consumer.org.hk/opw/"><img src="https://img.shields.io/badge/🛍 Online Price Watch-668B3F?&logo="></a>
</div>

<br>In the current climate of rising costs, many consumers are feeling the pinch at the checkout. If you are a budget-conscious shopper who frequently visits supermarkets, this scenario is likely all too familiar. Supermarkets often adjust their prices for various promotions, utilising a pricing strategy known as [Hi-Lo Pricing](https://en.wikipedia.org/wiki/High%E2%80%93low_pricing).

While it is possible to track these fluctuations manually, such an approach can be both time-consuming and exhausting. To alleviate this burden, we are pleased to introduce SuperPriceWatchdog, a tool designed to help you monitor supermarket prices effectively.

**First Published:** 15 February 2024  
**Last Updated:** 23 December 2024


<!-- ROADMAP -->
## Table of Contents
- [1 - Motivation: Maximise Savings with Effortless Deal Hunting](#1)
- [2 - When Should I Buy This Item?](#2)
    - [2.1 - Be Notified of Latest Deals](#2.1)
    - [2.2 - Price Alert Design](#2.2)
- [3 - Solution Architecture](#3)
    - [3.1 - System Design](#3.1)
- [4 - Contribute to This Project](#4)


<!-- SECTION 1 -->
<a name="1"></a>

## Motivation: Maximise Savings with Effortless Deal Hunting
SuperPriceWatchdog stands for **Super**market **Price** **Watchdog**. It is a Telegram bot designed to monitor the fluctuating prices of products across major supermarkets in Hong Kong. The goal of its creation is to help users save significant amounts of time and effort while searching for the best available deals.

Users receive daily notifications (except on Fridays) about exceptional offers on their selected items. As highlighted previously, SuperPriceWatchdog seeks to alleviate the hassle of constant price monitoring, allowing users to redirect their time and energy towards other pursuits.

<div align="center">
  <a href="https://forum.hkgolden.com/thread/7851001/page/1"><img src="./imgs/motivation.png" width="70%"></a>
  <p><i>a poor golden son didn't how to play the game.</i></p>
</div>


<!-- SECTION 2 -->
<a name="2"></a>

## When Should I Buy This Item?
SuperPriceWatchdog offers a variety of features to help you navigate the items you are interested in. Setting up a daily price alert is the bot's key function. Let’s explore how you can embark on this journey to savings.

<a name="2.1"></a>

### Be Notified of Latest Deals
Simply drop a message to [@SuperPriceWatchdog](https://t.me/SuperPriceWatchdogBot) on Telegram, and you will receive guidance on how to navigate the bot and set up your daily price alert.

> [!NOTE]  
> Latency is expected because SuperPriceWatchdog relies on community servers.

<div align="center">
  <a href="https://t.me/SuperPriceWatchdogBot"><img src="./imgs/demo.png" width="40%"></a>
</div>

<a name="2.2"></a>

### Price Alert Design
SuperPriceWatchdog collects 60 days of price records and calculates statistics for each item. When the unit price (after promotion) is below the first quartile, that item is considered a best deal. The price monitoring is entirely automated and requires minimal effort. Unlike the official price alerts, users are required to set a target price manually or adjust it based on intuition.

Promotion weeks start on Fridays, meaning most prices are refreshed every Friday (based on my past experience as a Red Label Supermarket employee). Unfortunately, the latest record of Online Price Watch (OPW) data refers to the previous day's prices and promotions. Prices in our database may not reflect the most current prices. Therefore, price alerts are disabled on Fridays.

<div align="center">
  <a href="https://t.me/SuperPriceWatchdogBot"><img src="./imgs/meme.png" width="45%"></a>
</div>


<!-- SECTION 3 -->
<a name="3"></a>

## Solution Architecture
SuperPriceWatchdog is built on top of the **Consumer Council**'s [OPW](https://data.gov.hk/en-data/dataset/cc-pricewatch-pricewatch) data, which provides the previous day's prices and promotions from supermarkets.

Each day, a data pipeline retrieves the data and stores it in a **PostgreSQL** database. The ETL process is executed using **Polars**, which efficiently handles data structure and manipulation. At the end of the pipeline, daily price alerts are triggered and sent to users. Through a webhook method, users interact with SuperPriceWatchdog on **Telegram**, sending requests to a **Flask** application that processes their messages.

Both the pipeline and the web application are hosted on a **PythonAnywhere** Beginner account, while the database is maintained on **Supabase**’s free tier. These providers generously offer serverless computing resources, making it possible to host and operate the service efficiently at no cost.

<div align="center">
  <a href="https://t.me/SuperPriceWatchdogBot"><img src="./imgs/solution_architect.png" width="60%"></a>
</div>

<a name="3.1"></a>

### System Design
To optimise for limited computing resources, we aim to minimise the complexity of development and deployment. The system consists of only 2 operational tasks: a scheduled data pipeline and a web application. Both scripts are version-controlled using Git and are implemented with CI/CD for efficient deployment.

In the current setup on PythonAnywhere, both the scheduled task and the website hosting on the Beginner account have an expiration period. I will regularly reactivate these services to ensure that SuperPriceWatchdog remains fully operational.

<div align="center">
  <a href="https://t.me/SuperPriceWatchdogBot"><img src="./imgs/system_design.png" width="50%"></a>
</div>


<!-- SECTION 4 -->
<a name="4"></a>

## Contribute to This Project
SuperPriceWatchdog is a free service hosted on community servers, designed to assist shoppers in finding the best deals across supermarkets. We invite you to get involved and contribute to the SuperPriceWatchdog project, enhancing the service for the community.

Your feedback, suggestions, and support can significantly improve our features and user experience. We encourage you to create issues in this repository to collaborate and share your ideas. Join us in our mission to empower consumers and maximise savings—together, we can make shopping smarter and more efficient for everyone!

<div align="center">
  <a href="https://t.me/SuperPriceWatchdogBot"><img src="./imgs/contribute.png" width="70%"></a>
</div>


<!-- MISCELLANEOUS -->
<a name="5"></a>

## Changelog
<details>
  <summary>[2.0.0] Revamped Version</summary>
  [2.0.1] 2024-12-23<br>
  Revamped data pipeline and Telegram bot.
  <h4>Added</h4>
  <ul>
    <li>Hosted the service serverless on PythonAnywhere and Supabase.</li>
    <li>Implemented CI/CD integration between PythonAnywhere and GitHub.</li>
  </ul>
  <h4>Changed</h4>
  <ul>
    <li>Enhanced user experience in the Telegram chat.</li>
    <li>Replatformed the database from SQLite to PostgreSQL.</li>
    <li>Transitioned from polling to webhook for Telegram interactions.</li>
    <li>Switched from Pandas to Polars for data processing.</li>
  </ul>
</details>

<details>
  <summary>[1.0.0] Project Initiation</summary>
  [1.0.1] 2024-02-15<br>
  Initial Repository.
</details>

## Product Backlog
This project is managed using a product backlog. You can review the [backlog](https://docs.google.com/spreadsheets/d/1hZBngU6REh5M9iyUclPlf8IyO3Iz3ZVW1exo_-vM1ks/pubhtml?gid=1316504644&single=true) to understand the prioritised list of features, changes, enhancements, and bug fixes planned for future development.

## License
This project is licensed under the MIT License. See the [LICENSE](./LICENSE) file for details. Feel free to fork and collaborate on the project!
