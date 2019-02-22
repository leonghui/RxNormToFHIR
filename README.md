# RxNormToFHIR

The script performs the following functions:
1. Look up each RxNorm concept against its relationships and attributes
2. Translate each concept into a FHIR Medication resource
3. Load resources as bundles into a target FHIR server (tested with [HAPI-FHIR] JPA Server 3.7.0).

RxNorm relationships processed include ([ref][UsingRxNormWithFHIR]):
* has_ingredient
* has_dose_form
* consists_of
* contains

RxNorm attributes processed include ([ref][RxNormBOSS]):
* RXN_BOSS_STRENGTH_NUM_VALUE
* RXN_BOSS_STRENGTH_NUM_UNIT
* RXN_BOSS_STRENGTH_DENOM_VALUE
* RXN_BOSS_STRENGTH_DENOM_UNIT

The script also normalizes pack concepts (term type = BPCK, GPCK) to its constituent clinical drug concepts (term type = SCD).

The goal is to create a FHIR server to be used as a dictionary backend for natural language processing (NLP).

Script output:
```
Reading RxNorm concepts file	4.421 s
Reading RxNorm relationships file	18.02 s
Reading RxNorm attributes file	18.65 s
Writing FHIR Medication resources	3.931 s
[main] INFO ca.uhn.fhir.util.VersionUtil - HAPI FHIR version 3.7.0 - Rev f2560071c8
[main] INFO ca.uhn.fhir.context.FhirContext - Creating new FHIR context for FHIR version [R4]
[main] INFO ca.uhn.fhir.util.XmlUtil - FHIR XML procesing will use StAX implementation 'Java Runtime Environment' version '1.8.0_201'
Loading SearchParameter bundle (size: 2) to server
0 remaining
8.971 s
Loading Substance bundle (size: 3326) to server
2326 remaining
<snip>
0 remaining
55.79 s
Loading Medication bundle (size: 45083) to server
44083 remaining
<snip>
0 remaining
Loading MedicationKnowledge bundle (size: 45083) to server
44083 remaining
<snip>
0 remaining
21.60 min

```

TODO
- [x] Load RxNorm ingredients as FHIR Substance resources
- [x] Load RxNorm ingredient synonyms as extensions in FHIR Substance resources
- [x] Load RxNorm drug brand names as extensions in FHIR Medication resources
- [x] Load RxNorm drug parent concepts in FHIR MedicationKnowledge resources
- [x] Load RxNorm drug synonyms in FHIR MedicationKnowledge resources

See also:
[MedServe][]



[HAPI-FHIR]: https://github.com/jamesagnew/hapi-fhir
[UsingRxNormWithFHIR]: http://wiki.hl7.org/index.php?title=Using_rxNorm_with_FHIR
[RxNormBOSS]: https://www.nlm.nih.gov/pubs/techbull/so18/brief/so18_rxnorm_boss.html
[MedServe]: https://github.com/AuDigitalHealth/medserve
