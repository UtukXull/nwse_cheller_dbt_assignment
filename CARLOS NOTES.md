# Carlos Notes

In this document I will write all the relevant information I would like to communicate to the evaluation team regarding the choices I made during the time working on the assignment and any blockers or clarifications I would like to get across.

## Conventions, assumptions and decisions:

### **Naming Conventions and Data Types enforcing**
   - All fields are formatted as snake case.
   - The PK is always the first field in the SELECT statement.
   - FKs are the next group of fields after the PK and always point to the primary key of the foreign table by separating the foreign primary key name from other text with a double underscore.
   - Dates always begin with the word "date" and are always presented as a local time zone timestamp.
      - I am assuming that all the provided timestamps are initially stored in Los Angeles time zone (as stated in the user table) and I'm converting all of them to Berlin time zone.
   - Binary value fields always begin with a verb (is_, has_, etc.) and are always converted to boolean data types.

### **Workload decisions**
   - For this assignment I have chosen to work around the Salesforce opportunities and their surrounding related tables.
      - There could be a path for building another star schema around the case table, which relates among others to product, price and solution, but I will choose to focus on the opportunity and opportunity_history tables given the time constraints and the fact that they have more metrics to work with.

### **Notes about working on the opportunity star schema**

#### Ignored tables
   - Campaign
      - campaign_id from opportunity table doesn't have a match with campaign table
   - Lead
      - Could not find a valid connection to or from the lead table
   - Contact
      - Could not find a valid connection between the contact table and the opportunity and user tables

#### Testing
   - Referential integrity has been tested successfully
   - Testing found no duplication in the marts data
   - Most of the tests I could come up with succeeded or allowed me to address development mistakes until they did, except for one:
      - There is a potential data quality issue where in half of the opportunity_history rows, the closed_date is earlier than the created_date. I believe that if my assumption regarding the fields is correct, it is also correct for the data quality test to fail and flag this issue, so I will leave it in a failed state.
