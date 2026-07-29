# changelog

## 07/28

- **backend optimizations**

  - reduced requests to redis database by ***90-95%***

  - runs agent.py locally, which does polling and backfilling without relying on serverless

- terminal dump cleanup

- removed task duplication in the completed tasks

## 07/27

### **MAJOR FEATURE: OPERATIONAL VERCEL DEPLOYMENT!!**

- **working completed tasks section**

- **notification center**

- selectable polling frequency (15s to 1h), manual refresh, and manual backfill

- multi-toggle buttons to fast-select recording/idle/etc.

- automatic throttling of hourly polling from 7pm to 9am PDT

- "last updated" on live rig status, "last seen online" on offline rigs

- added servicing rigs

- resolved alerts stay listed, just no longer red

- "email me a code" button now always displays

- added "updated [timestamp]" messages

- renamed idling to idle (green)

- stat boxes now match the card behind them

- fixed UTC time zone bug counting recordings after 5pm towards the next day

- fixed idle/servicing flickering to 0 when API drops

- updated aggregate colors/opacity for better UX

- github cleanup

- changelog

- **wrote description for release 1.0.0**

## 07/26

- starting vercel migration

- horizontally scrollable aggregate graphs

## 07/25

- **small window improvements**

- **push notifs and sound effects (harrison)**
    - when a pi stops recording
    - cam disconnecting or frame health drops below 95%
    - storage drops below 10%/overheating above 70C

- fixed terminal auto-scroll

- implemented alert cooldown to prevent terminal clutter

- email and password login

- more intuitive sorting

## 07/24

- sorting by alphabetical, storage, status (armon)

- color coded rig selection

- weekends excluded from graphs

## QUEUED FEATURES

- add location to pis, save presets to selected pis

- selectable color themes

- export completed tasks to csv/txt

- button to turn off polling

- 2 stat boxes side by side for mobile

- mobile notifications (no service worker?)

- show email button by default

- mute sounds

- saved passwords

###### last updated 07/27 11:35AM