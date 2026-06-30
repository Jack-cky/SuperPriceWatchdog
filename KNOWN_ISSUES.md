# Known Issues

## Issue: Unavailability of Online Price Watch Data from DATA.GOV.HK
### Description
The historical data API provided by data.gov.hk is expected to return a data file for each day. However, it has been observed that the API often fails to return data files for certain periods, leading to gaps in the historical data.
### Affected Areas
- Historical data retrieval functionality.
- SuperPriceWatchdog relies on complete daily datasets.
### Symptoms
- Missing data files for specific dates when querying the historical data API.
- Inconsistent data availability, which affects data integrity and analysis.
### Workarounds
- **Disable Price Alert**: No alert will be sent out when the upstream data is unavailable.
- **Record Incomplete Dates**: Log how often the API fails to provide daily data for record-keeping.
### Status
- ~~No ticket has been raised with data.gov.hk to address this issue, and we will monitor how long the problem persists.~~
- A feedback form (Ref: [1746966652-A-GG748](imgs/Feedback%20Form%201746966652-A-GG748.png)) was submitted to data.gov.hk regarding the issue on May 11, 2025.
- The data is now available on data.gov.hk after the feedback form was submitted.
- It was found that the data is available only through a different API endpoint, which is not specified in the API documentation.
### Appendix
<div align="center">
  <a href="https://youtu.be/93j2xe2Y0KM?si=YGZBWeZyacA2L8s-&t=30"><img src="imgs/issue_opw.png" width="70%"></a>
  <p><i>only 4 records are returned while 14 are expected.</i></p>
</div>
