# rumqttd 0.12.4 (Feb 1, 2023)
- Client id with tenant prefix should be set globally (#564) 

# rumqttd 0.12.3 (Jan 23, 2023)
- Restructure AlertsLink and MetersLink to support writing to Clickhouse (#557)
- Add one way bridging support via BridgeLink (#558)

# rumqttd 0.12.2 (Jan 16, 2023)
- Add AlertLink to get alerts about router events (#538)
- Add basic username and password authentication (#553)
- Don't allow client's with empty client_id (#546)
- Disconnect existing client on a new connection with similar client_id (#546)
- Skip client certificate verification when using native-tls because it doesn't support it (#550)


---

Old changelog entries can be found at [CHANGELOG.md](../CHANGELOG.md)
