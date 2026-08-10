# changelog

## 07/30

- filtered, sorted, and optimized data dump python script 

- operational feedback form

- **fixed aggregate hours time discrepancy**

- "new changes" popup on startup, backed by `new_changes.md`

- notifications no longer disappear from the notification center

- better recording-stopped notifications

- notification bell moves to the left side of the top bar on small screens

- task name added back to the terminal dump on a recording stop

## 07/29 

- previous task/operator/time displayed on non-recording pis

- fixed bug where completed tasks reverse button would collapse the section

- wrote python script to do data dump into csv and filter for special tasks

- ranking dropdowns

- **wrote release 1.1.0 notes** 

## 07/28

- **backend optimizations**

  - reduced requests to redis database by ***90-95%***

  - runs agent.py locally, which does polling and backfilling without relying on serverless

- terminal dump cleanup

- removed task duplication in the completed tasks

- feedback form

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

- **migrate to web socket**

- fix disappearing alerts issue

- fix rankings (why is there an Unknown operator?)

- add back task name to terminal dump

- hours of tasks per type
  - break it down into by day
  - x`rig operator & an actual task name

- add location to pis, save presets to selected pis

- selectable color themes

- export completed tasks to csv/txt

- option to have AM/PM displayed in terminal dump

- custom sound effects 

###### last updated 07/29 10:22AM