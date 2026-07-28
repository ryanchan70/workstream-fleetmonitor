# changelog

## 07/27

### **MAJOR FEATURE: OPERATIONAL VERCEL DEPLOYMENT!!**

- **fixed the UTC bug** — all dates, totals and log timestamps are now Pacific,
  so the day no longer rolls over at 5pm and Friday evenings stop counting as weekend

- **fixed idle/servicing flickering to 0** — servicing survives the API dropping
  it for a poll or two, and preview never surfaces as a status

- selectable polling frequency (15s to 1h), manual refresh, and manual backfill

- automatic hourly polling from 7pm to 9am Pacific

- "last updated" on live rig status, "last seen online" on offline rigs

- notification center

- resolved alerts stay listed, just no longer red

- "email me a code" button 

- **status buttons moved up beside the sort control**, colour-coded, and no longer
  disappear when a status happens to be empty

- renamed idling to idle (green)

- stat boxes now match the card behind them

- **working completed tasks section**

- updated aggregate colors/opacity

- github cleanup

- added servicing rigs

- changelog

- multi-toggle buttons

- wrote description for release 1.0.0

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