The `CEN.csv` and `MSR.csv` datasets were derived from the [Union Army Data Set](https://www.nber.org/research/data/union-army-data-set) for use in the paper titled ["Optimal F-Score Clustering for Bipartite Record Linkage"](https://arxiv.org/pdf/2311.13923.pdf).

From the paper:

> "The Union Army data comprise a longitudinal sample of Civil War veterans collected as part of the
Early Indicators of Aging project (Fogel et al., 2000). Records of soldiers from 331 Union companies
were collected and carefully linked to a data file comprising military service records—which we call
the MSR file—as well as other sources. These records also were linked to the 1850, 1860, 1900,
and 1910 censuses. The quality of the linkages in this project is considered very high, as the true
matches were manually made by experts (Fogel et al., 2000). Thus, the Union Army data file can
be used to test automated record linkage algorithms."

> "We consider re-linking soldiers from the MSR data to records from the 1900 census, which we
call the CEN data file. This linkage problem is difficult for automated record linkage algorithms due
to the presence of soldiers’ family members in the CEN data. Furthermore, not all soldiers from
the MSR data have a match in the CEN data. However, we can consider the linkages identified by
the Early Indicators of Aging project as truth. For the linking fields, we use first name, last name,
middle initial, and approximate birth year."

Note that the `CEN.csv` file only contains:
1. The 1900 census records of soldiers who match `MSR.csv` records in the Union Army dataset.
2. The 1900 census records of family members of these soldiers.

The `CEN.csv` and `MSR.csv` files contain no duplication within, only duplication between them. The task is to identify matches between the two datasets, accounting for noise in recorded names and birth dates, and avoiding false match with family members that may share similar names.

A unique identifier recorded in the `label_true` attribute can be used to identify matches. Some records have an `NA` value for this unique identifier, since they were out of the scope of the linkage performed for the Union Army Dataset.
