# egs-collectors

Scheduled data collectors for [EveryGameStat](https://everygamestat.com).

- `collect.mjs` (workflow **FiveM servers**, every 30 minutes): reads the public FiveM server list
  that the FiveM client itself uses (Cfx.re), counts players per server and sends the servers that
  have players to EveryGameStat. Player names are never read or stored. Counts are as reported by
  each server.
- `keepalive.yml`: one small commit a week, because GitHub pauses scheduled workflows in public
  repositories after 60 days without activity.
