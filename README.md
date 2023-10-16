# shyftplan-scripts
<p align="center">
 <img src="diagram.png?raw=true" Logo" width="90%" height="90%" />
</p>

This repository is used to automate:
1. part of procesess we have in our sheduling app - **Shyftplan**
2. account creation in Shyftplan and our order delivery app - **Onfleet**
3. sending all data about workes and their status to operation database - **Airtable**
4. use other apps like SerwerSMS to inform new workers about their recruitment status

Just some of these processes related to **the Shyftplan application alone took us more than 160h** a month 
on just using the application through the GUI.

# why it took 160 hours a month?

Our backgroud:
- we are No. 1 q-commerce in Poland
- we have about 500 physical workers
- we manage over 30 locations in 5 different polish cities

Key issues with our current process:
- early access to schedules for 50% workers
- fixed schedule for 30% workers
- 70% of all need flexible shedules
- possibility of using company equipment depending on worker status
- 2 different projcets for workers + supporting external company in delivery
- more and more...


90% of the activities in shyftplan were repetitive because of which I decided to automate it.

# Examples

### First
Every new employee need to have accounts in Shyftplan and OnFleet with proper settings.
You can do it manually or you can use my make_accounts.py which does everything for you and sends the relevant information to Airtable and the new employee.


### Second
The process of re-clicking shifts from status A to status B 
(this process required a lot of concentration and was exhausting) 
took 2h/week + it was a repetitive process.

In response to the need, I wrote a script that did it for us in 10minutes, 
which additionally ran in the background ----> "reclicking_shedule.py" in /scripts_first_steps/JUSH

### Third
Each week the schedule must be published.
When you do it manually you do the following things:
1. you go to the appropriate location
2. you select the appropriate week
3. You click "publish"
4. you confirm

And the same thing 30 times/ every week :)

I wrote simple script shiftplans_publishing.py, which does it for you.

# quality of code
Since my learning of programming started not so long ago (before that I mainly used no-code/low-code tools like Zapier),
therefore I made a classification of my tools into two categories:

1. scripts_first_steps
2. scripts_goodlooking

As the name suggests, the first folder contains my first steps in scripts writing,
95% of them are non-objective and may resemble noodles, but these tools works very well for my needs
(their downside is that they are non-objective and hard to develop especially for someone who hasn't worked with them).

The second folder contains code that is already object-oriented and one that is easily expandable.


