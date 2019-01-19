# RxTermsToFHIR

The script performs the following functions:
1. Look up each RxTerms concept against its RxNorm relationships and attributes
2. Translate each concept into a FHIR Medication resource
3. Load resources as bundles into a target FHIR server (tested with [HAPI-FHIR] JPA Server 3.6.0).

RxNorm relationships processed include ([ref][UsingRxNormWithFHIR]):
* has_ingredient
* has_dose_form

RxNorm attributes processed include ([ref][RxNormBOSS]):
* RXN_BOSS_STRENGTH_NUM_VALUE
* RXN_BOSS_STRENGTH_NUM_UNIT
* RXN_BOSS_STRENGTH_DENOM_VALUE
* RXN_BOSS_STRENGTH_DENOM_UNIT

The script also normalizes pack concepts (term type = BPCK, GPCK) to its constituent clinical drug concepts (term type = SCD).

The goal is to create a FHIR server to be used as a dictionary backend for natural language processing (NLP).

Script output:
```
Reading RxNorm concepts file    1.297 s
Reading RxNorm relationships file    6.595 s
Reading RxNorm attributes file    6.534 s
Reading RxTerms file    145.8 ms
[main] INFO ca.uhn.fhir.util.VersionUtil - HAPI FHIR version is: 3.6.0
[main] INFO ca.uhn.fhir.context.FhirContext - Creating new FHIR context for FHIR version [DSTU3]
[main] INFO ca.uhn.fhir.util.XmlUtil - FHIR XML procesing will use StAX implementation 'Java Runtime Environment' version '1.8.0_201'
Loading Medication bundle to server    2.497 s
Loading Substance bundle to server    278.5 ms

```

TODO
- [x] Load RxNorm ingredients as FHIR Substance resources
- [ ] Load RxNorm parent concepts as extensions in FHIR Medication resources
- [ ] Load RxNorm brand names as extensions in FHIR Medication resources

See also:
[MedServe][]



[HAPI-FHIR]: https://github.com/jamesagnew/hapi-fhir
[UsingRxNormWithFHIR]: http://wiki.hl7.org/index.php?title=Using_rxNorm_with_FHIR
[RxNormBOSS]: https://www.nlm.nih.gov/pubs/techbull/so18/brief/so18_rxnorm_boss.html
[MedServe]: https://github.com/AuDigitalHealth/medserve
