Original problem statement: https://www.cs.duke.edu/courses/fall15/compsci290.1/Assignments/assignment1.html

WhiteHouse.scala works on White House visitor logs (found at https://www.whitehouse.gov/briefing-room/disclosures/visitor-records)
to determine the following.
    1) The 10 most frequent visitors (NAMELAST, NAMEFIRST, NAMEMID) to the White House.
    2) The 10 most frequently visited people (visitee_namelast, visitee_namefirst) in the White House.
    3) The 10 most frequent visitor-visitee combinations.
In WhiteHouseData is a smaller test.csv (5000 rows) of the 2015 data.

Wikipedia.scala works on Wikipedia page data (found at http://haselgrove.id.au/wikipedia.html) to answer the
following questions:
    1) What pages have no outlinks?
    2) What pages have no inlinks?
In WikipediaData are smaller test files.

Included are an RDD version and a SparkSQl version.