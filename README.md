# Telco Customer Churn — Never Gonna Let You Churn! :two_hearts:
## :punch: Reached 78% of churn customers :smoking::yen: Achieving 55% of ROI for a customer retention programme
Click [me](https://www.kaggle.com/code/jackkyc/55-roi-never-gonna-let-you-churn) to travel Kaggle Notebook!

<center>
    <a href="https://youtu.be/dQw4w9WgXcQ">
        <img width="500px" height="500px" src="https://github.com/Jack-cky/TCC-Never_Gonna_Let_You_Churn/blob/main/imgs/kaggle_never_gonna_let_you_churn.gif"/>
    </a>
</center>

---
### Abstract
- About 27% of customers stopped using the company's offerings in the "last" month
- XGB classifier is used as the final prediction model with oversampling method (dev performance: accuracy: 78%; precision: 56%; recall: 73%; f1: 64%)
- Attributes `Partner`, `Dependents`, `Contract` and `InternetService` are viewed as important in the eyeball
- Attributes `MonthlyCharges`, `tenure` and `TotalCharges` are evaluated as important from the model
- 78% of churn customers receive a one-time discount promotion which achieves 55% of ROI on the customer retention programme

---
### Data Source
- "Telco Customer Churn" on Kaggle provides a dataset from a Telco company to analyse customer data and develop solutions to aid customer retention programs
- Without a doubt, the dataset is very ideal and not common in reality
- Similar to the Titanic dataset, this data source is targeted for technical demonstration and doesn't mean anything

---
### Exploratory Data Analysis
![EDA](https://github.com/Jack-cky/TCC-Never_Gonna_Let_You_Churn/blob/main/imgs/tableau_dashboard.png)
- Exploratory data analysis is done in [Tableau](https://public.tableau.com/app/profile/jackcky/viz/TCC_EDA/EDA) for enabling dynamic option to drill-in individually

---
### EDA Insights
- (Top-Right) About 27% of customers stopped using the services from the "last" month, which implies an imbalance in the response variable
- (Middle) Some attributes (`Partner`, `Dependents`, `Contract` and `InternetService`) show obvious divergencies in churn status, which indicates the importance of these
- (Bottom-Left) Customers who used the services for a long period are less likely to switch to other competitors' offerings
- (Bottom-Right) A non-linear phenomenon is observed between `MonthlyCharges` and `TotalCharges`, which means customers are switching within services

---
### Machine Learning Implementation
| Model | Accuracy | Precision | Recall | F1-score |
| --- | --- | --- | --- | --- |
| Bleeding Heart | 26.58% | 26.58% | 100% | 41.99% |
| Logistic Regression | 75.71% | 52.86% | 79.53% | 63.51% |
| Random Forest | 77.37% | 55.36% | 76.56% | 64.26% |
| eXtreme Gradient Boosting | 77.84% | 56.39% | 73.29% | 63.74% |
- XGB model is used as final model for it beats other models in terms of precision on the dev set

---
### Log
- Version II (2022-09-22): Changed project theme to hilarious one due to no view on Kaggle :disappointed_relieved:
- Version I  (2022-09-12): Commited initial version for describing the churn prediction with business rationale

---
### Acknowledgements
- Project theme is inspired from the [YouTube video](https://youtu.be/dQw4w9WgXcQ)
