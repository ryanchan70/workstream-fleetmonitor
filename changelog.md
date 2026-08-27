# changelog

## 08/26
- added pi status to ranked pi timing to tell if tasks are outdated because a pi is offline
- made data capture links more obvious + upload status

## 08/25
- added v2.0 and v2.1 buttons to select a whole hardware generation at once (rpi5 = v2.0, rock5c = v2.1)
- added AM/PM or 24hr toggle for entire page
- if API and estimates are within 5 seconds, assumes API is correct
- added hyperlinks straight to **Data Capture**

## 08/24
- removed y-intercept from linear regression (a recording with no time should be 0 bytes)

## 08/21
- added value labels to hours recorded per day

## 08/19

- code cleanup & reorganization

## 08/18

- fixed duration estimates using bytes

  - changed from averaging -> K-nearest neighbors -> linear regression

- every recording in the ranked timings and completed tasks shows the estimate first and the API's own duration in parentheses after, colored grey / amber / red for 0-5% / 5-20% / 20%+ discrepancy

- aggregate fleet stats now   has a Estimates | API toggle (estimated durations in aquamarine, API durations in blue)

## 08/13

- only shows notes button if there are notes for pis

## 08/12

- added notes

- diagnosing issues with wrong API recording duration and byte estimations

## 08/11

- **fixed and implemented time estimates based on bytes**

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

- long-term: add scheduling integration

- long-term: add multiple programs if given permission

- option to adjust time zone

- more obvious toggles at the top of page

- hours of tasks per type
  - break it down into by day

- add location to pis, save presets to selected pis

- selectable color themes

- export completed tasks to csv/txt

- custom sound effects 

###### last updated 08/25 2:28PM PDT