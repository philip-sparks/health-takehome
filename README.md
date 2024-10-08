# Handling Health Data

## Contributing

## How do you handle the file size and format efficiently when the uncompressed file will exceed memory limitations on most systems?

Originally, I tried to read the file natively into Spark, which took too long. Then I looked into explicitly defining the schema, which did not take as much time but took a while to create in Scala. Next, I decided to parse the file into a Parquet file using pure Python. That compresses well, but then seems to explode when you try reloading it back into Spark.

Finally, I decided to not get cute but directly parse what the question is asking for in Python. Using iterative-json (ijson) allows you to chunk the JSON without loading it all into memory. This runs in seconds, making me concerned why the other methods were taking so long.

## When you look at your output URL list, which segments of the URL are changing, which segments are repeating, and what might that mean?

Most of the base domain remains constant, where `https://anthembcbs[state].mrf.bcbs.com/` is stable, but the state code changes. Given that none of the state codes are "ny", it makes me concerned if I've truly extracted the correct values based on the description JSON field.

In addition to that, the Key-Pair-Id is always constant, while the Signature value is always unique.

There are two main types of filenames: `301_71A0_in-network-rates` and `800_72A0_in-network-rates`. Each one comes in two parts.

The URLs are valid and lead to more data about negotiated rates for medical services of the reporting entity name. These reporting entity names do indeed come from New York, which validates the parser.

## Is the 'description' field helpful? Is it complete? Does it change relative to 'location'? Is Highmark the same as Anthem?

The description field is definitely helpful overall. Without it, it would be extremely difficult to determine what type of care plan the corresponding URLs contained. It's unclear if it is actually complete. After collecting 18,642,871 pairs of descriptions and locations (I cut the full run short, that file was already > 9 GB), I found 122 distinct descriptions and 11,623 distinct locations (URLs). Clearly, the descriptions are rather general, as there are many more location URLs than descriptions. Given the millions of pairs, I'm curious as to whether or not there is a large amount of duplication happening in the index file.

Googling "Highmark vs Anthem", these appear to be two sister companies under the umbrella of Blue Cross Blue Shield.

## Anthem has an interactive MRF lookup system. This lookup can be used to gather additional information - but it requires you to input the EIN or name of an employer who offers an Anthem health plan: [Anthem EIN lookup](https://www.anthem.com). How might you find a business likely to be in the Anthem NY PPO? How can you use this tool to confirm if your answer is complete?

Within the anthem_index file, there are EINs that correspond to reporting plans within the file. My best guess is that you can use this number in the Anthem EIN lookup link and see if the number of reports in the nested JSON match the search results coming from Anthem search results in the browser. I did not confirm this, as I downloaded June 2024 data and did the lookup in July 2024, so it is likely the results would not be the same.

## Duration of time spent

- **Day 1:** 90 minutes (Parsing, Coding, Debugging Primarily)
- **Day 2:** 180 minutes (Debugging, Coding, Write-Up)
