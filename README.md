# RxNormToFHIR

The script performs the following functions:
1. Look up each RxNorm concept against its relationships and attributes
2. Translate each concept into a pair of FHIR Medication and MedicationKnowledge resources
3. Load resources as bundles into a target FHIR server (tested with [HAPI-FHIR] JPA Server 3.7.0) or into JSON files for local testing.

RxNorm relationships processed include ([ref][UsingRxNormWithFHIR]):
* has_ingredient
* has_dose_form
* consists_of
* contains
* isa
* has_form

RxNorm attributes processed include ([ref][RxNormBOSS]):
* RXN_BOSS_STRENGTH_NUM_VALUE
* RXN_BOSS_STRENGTH_NUM_UNIT
* RXN_BOSS_STRENGTH_DENOM_VALUE
* RXN_BOSS_STRENGTH_DENOM_UNIT
* RXN_STRENGTH

The goal is to create a FHIR server to be used as a dictionary backend for natural language processing (NLP).

Script output:
```
Reading RxNorm concepts file	4.522 s
Reading RxNorm relationships file	13.07 s
Reading RxNorm attributes file	14.38 s
Writing FHIR Substance resources	312.3 ms
Writing FHIR Medication resources	5.154 s
Writing FHIR SearchParameter resources	88.20 ms
[main] INFO ca.uhn.fhir.util.VersionUtil - HAPI FHIR version 3.7.0 - Rev f2560071c8
[main] INFO ca.uhn.fhir.context.FhirContext - Creating new FHIR context for FHIR version [R4]
Writing Substance bundle (size: 6913) to local file	4.416 s
Writing Medication bundle (size: 90723) to local file	6.040 s
Writing MedicationKnowledge bundle (size: 90723) to local file	6.793 s
Units of measure detected: [%, ACTUAT, AU, BAU, CELLS, HR, MCI, MEQ, MG, ML, MMOL, PNU, SQCM, UNT]
Total time taken 6.801 s

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
