package main.groovy

import ca.uhn.fhir.context.FhirContext
import ca.uhn.fhir.rest.client.api.IGenericClient
import com.google.common.base.Stopwatch
import com.google.common.collect.*
import org.hl7.fhir.r4.model.*
import org.hl7.fhir.r4.model.Bundle.BundleType
import org.hl7.fhir.r4.model.Bundle.HTTPVerb
import org.hl7.fhir.r4.model.Medication.MedicationIngredientComponent

Stopwatch watch = Stopwatch.createStarted()

int CONNECT_TIMEOUT_SEC = 10
int SOCKET_TIMEOUT_SEC = 180

String FHIR_SERVER_URL = 'http://<server>:<port>/baseDstu3'
String RXNORM_SYSTEM = 'http://www.nlm.nih.gov/research/umls/rxnorm'
String RXNORM_VERSION = '12032018'
String RXNORM_FOLDER_NAME = "RxNorm_full_$RXNORM_VERSION"


// rxNorm concepts
// rxCui, CodeableConcept
Map<String, CodeableConcept> ingredients = [:]
Map<String, CodeableConcept> brandNames = [:]
Map<String, CodeableConcept> doseForms = [:]
Map<String, CodeableConcept> rxNormConcepts = [:]

// rxNorm one-to-one relationships
// source rxCui, target rxCui
Map<String, String> hasDoseForm = [:]

// rxNorm one-to-many relationships
// source rxCui, target rxCui
Multimap<String, String> hasIngredient = HashMultimap.create()
Multimap<String, String> consistsOf = HashMultimap.create()
Multimap<String, String> contains = HashMultimap.create()

// rxNorm attributes
// rxCui, ATN, ATV
Table<String, String, String> attributes = HashBasedTable.create()

// rxNorm term type
// rxCui, TTY
Map<String, String> rxNormTty = [:]

// rxNorm synonyms
// rxCui, Term
SetMultimap<String, String> rxNormSynonyms = HashMultimap.create()

// data structures to store FHIR resources
// FHIR ID, Resource
Map<String, Medication> medications = [:]
Map<String, MedicationKnowledge> medicationKnowledgeMap = [:]
Map<String, Substance> substances = [:]

// data structures to store custom search parameters
List<SearchParameter> parameters = []

Closure logStart = { String job ->
    watch.reset().start()
    print job + ('\t')
}

Closure logStop = {
    println watch.toString()
}

Closure readRxNormConceptsFile = {

    logStart('Reading RxNorm concepts file')

    FileReader cpcRxnConso = new FileReader("src/main/resources/$RXNORM_FOLDER_NAME/rrf/RXNCONSO.RRF")

    cpcRxnConso.eachLine { String line, int number ->

        List<String> tokens = line.split(/\|/, 19)

        /* 0	RXCUI
         * 1	LAT
         * 2	TS
         * 3	LUI
         * 4	STT
         * 5	SUI
         * 6	ISPREF
         * 7	RXAUI
         * 8	SAUI
         * 9	SCUI
         * 10	SDUI
         * 11	SAB
         * 12	TTY
         * 13	CODE
         * 14	STR
         * 15	SRL
         * 16	SUPPRESS
         * 17	CVF
         */

        // only consider non-suppressed RxNorm concepts
        if (tokens.get(11) == "RXNORM" && (tokens.get(16) == "N" || tokens.get(16) == "")) {
            switch (tokens.get(12)) {
                case 'DF': // RXCUI, STR
                    CodeableConcept doseForm = new CodeableConcept()
                            .addCoding(new Coding(RXNORM_SYSTEM, tokens.get(0), tokens.get(14)))
                    doseForms.put(tokens.get(0), doseForm)
                    break
                case 'IN': // RXCUI, STR
                    CodeableConcept ingredient = new CodeableConcept()
                            .addCoding(new Coding(RXNORM_SYSTEM, tokens.get(0), tokens.get(14)))
                    ingredients.put(tokens.get(0), ingredient)
                    break
                case 'BN': // RXCUI, STR
                    CodeableConcept brandName = new CodeableConcept()
                            .addCoding(new Coding(RXNORM_SYSTEM, tokens.get(0), tokens.get(14)))
                    brandNames.put(tokens.get(0), brandName)
                    break
                case ['SCD', 'SBD', 'GPCK', 'BPCK']: // RXCUI, STR
                    CodeableConcept concept = new CodeableConcept()
                            .addCoding(new Coding(RXNORM_SYSTEM, tokens.get(0), tokens.get(14)))
                    rxNormConcepts.put(tokens.get(0), concept)
                    rxNormTty.put(tokens.get(0), tokens.get(12))
                    break
            }

            // only consider non-suppressed synonyms
            if (tokens.get(16) == "N" || tokens.get(16) == "") {
                switch (tokens.get(12)) {
                    case 'SY': // RXCUI, STR
                        rxNormSynonyms.put(tokens.get(0), tokens.get(14))
                        break
                }
            }
        }
    }

    cpcRxnConso.close()
    logStop()
}

Closure readRxNormRelationshipsFile = {
    logStart('Reading RxNorm relationships file')
    FileReader cpcRxnRel = new FileReader("src/main/resources/$RXNORM_FOLDER_NAME/rrf/RXNREL.RRF")

    cpcRxnRel.eachLine { String line, int number ->

        List<String> tokens = line.split(/\|/, 17)

        /* 0	RXCUI1
         * 1	RXAUI1
         * 2	STYPE1
         * 3	REL
         * 4	RXCUI2
         * 5	RXAUI2
         * 6	STYPE2
         * 7	RELA
         * 8	RUI
         * 9	SRUI
         * 10	SAB
         * 11	SL
         * 12	DIR
         * 13	RG
         * 14	SUPPRESS
         * 15	CVF
         */

        // only consider non-suppressed RxNorm relationships
        if (tokens.get(10) == "RXNORM" && (tokens.get(14) == "N" || tokens.get(14) == "")) {
            switch (tokens.get(7)) {
                case 'has_ingredient':
                    hasIngredient.put(tokens.get(4), tokens.get(0))
                    break
                case 'consists_of':
                    consistsOf.put(tokens.get(4), tokens.get(0))
                    break
                case 'has_dose_form':
                    hasDoseForm.put(tokens.get(4), tokens.get(0))
                    break
                case 'contains':
                    contains.put(tokens.get(4), tokens.get(0))
            }
        }
    }

    cpcRxnRel.close()
    logStop()
}

Closure readRxNormAttributesFile = {
    logStart('Reading RxNorm attributes file')
    FileReader cpcRxnSat = new FileReader("src/main/resources/$RXNORM_FOLDER_NAME/rrf/RXNSAT.RRF")

    cpcRxnSat.eachLine { String line, int number ->

        List<String> tokens = line.split(/\|/, 14)

        /* 0	RXCUI
         * 1	LUI
         * 2	SUI
         * 3	RXAUI
         * 4	STYPE
         * 5	CODE
         * 6	ATUI
         * 7	SATUI
         * 8	ATN
         * 9	SAB
         * 10	ATV
         * 11	SUPPRESS
         * 12	CVF
         */

        String attribName = tokens.get(8)

        // only consider non-suppressed RxNorm attributes
        if (tokens.get(9) == "RXNORM" && (tokens.get(11) == "N" || tokens.get(11) == "")) {
            switch (attribName) {
                case [
                        'RXN_BOSS_STRENGTH_NUM_VALUE',
                        'RXN_BOSS_STRENGTH_NUM_UNIT',
                        'RXN_BOSS_STRENGTH_DENOM_VALUE',
                        'RXN_BOSS_STRENGTH_DENOM_UNIT'
                ]: // RXCUI, ATN, ATV
                    attributes.put(tokens.get(0), attribName, tokens.get(10))
                    break
            }
        }
    }

    cpcRxnSat.close()
    logStop()
}

Closure<BackboneElement> getMedicationIngredientComponent = { String scdc_rxCui, boolean forKnowledge ->
    String ing_rxCui = hasIngredient.get(scdc_rxCui).first()    // assume each component has only one ingredient

    CodeableConcept ingredient = ingredients.get(ing_rxCui)

    BackboneElement component; if (ingredient) {
        if (forKnowledge) {
            component = new MedicationKnowledge.MedicationKnowledgeIngredientComponent()
        } else {
            component = new MedicationIngredientComponent()
        }

        Substance substance = new Substance()

        substance.setStatus(Substance.FHIRSubstanceStatus.ACTIVE)
        substance.setCode(ingredients.get(ing_rxCui))

        String substanceId = "rxNorm-$ing_rxCui"    // use rxNorm-<rxCui> as resource ID
        substance.setId(substanceId)

        substances.put(substanceId, substance)

        if (component instanceof MedicationIngredientComponent) {
            component = (MedicationIngredientComponent) component

            Reference substanceReference = new Reference(substance)
            component.setItem(substanceReference)

            Map<String, String> scdcAttributes = attributes.row(scdc_rxCui)

            if (scdcAttributes) {
                String denominatorUnit = scdcAttributes.get('RXN_BOSS_STRENGTH_DENOM_UNIT')
                Double denominatorValue = scdcAttributes.get('RXN_BOSS_STRENGTH_DENOM_VALUE').toDouble()
                String numeratorUnit = scdcAttributes.get('RXN_BOSS_STRENGTH_NUM_UNIT')
                Double numeratorValue = scdcAttributes.get('RXN_BOSS_STRENGTH_NUM_VALUE').toDouble()

                Ratio amount = new Ratio()
                        .setNumerator(new Quantity().setValue(numeratorValue).setUnit(numeratorUnit))
                        .setDenominator(new Quantity().setValue(denominatorValue).setUnit(denominatorUnit))

                component.setStrength(amount)
            }

            component.setIsActive(true)

        } else if (component instanceof MedicationKnowledge.MedicationKnowledgeIngredientComponent) {
            component = (MedicationKnowledge.MedicationKnowledgeIngredientComponent) component

            Reference substanceReference = new Reference(substance)
            component.setItem(substanceReference)

            Map<String, String> scdcAttributes = attributes.row(scdc_rxCui)

            if (scdcAttributes) {
                String denominatorUnit = scdcAttributes.get('RXN_BOSS_STRENGTH_DENOM_UNIT')
                Double denominatorValue = scdcAttributes.get('RXN_BOSS_STRENGTH_DENOM_VALUE').toDouble()
                String numeratorUnit = scdcAttributes.get('RXN_BOSS_STRENGTH_NUM_UNIT')
                Double numeratorValue = scdcAttributes.get('RXN_BOSS_STRENGTH_NUM_VALUE').toDouble()

                Ratio amount = new Ratio()
                        .setNumerator(new Quantity().setValue(numeratorValue).setUnit(numeratorUnit))
                        .setDenominator(new Quantity().setValue(denominatorValue).setUnit(denominatorUnit))

                component.setStrength(amount)
            }

            component.setIsActive(true)
        }

        return component

    }
}

Closure writeMedicationResources = {
    logStart('Writing FHIR Medication resources')

    rxNormConcepts.keySet().each { rxCui ->

        Medication med = new Medication()
        MedicationKnowledge medKnowledge = new MedicationKnowledge()

        String tty = rxNormTty.get(rxCui)

        switch (tty) {
            case ['SBD']:
                //med.setIsBrand(true)
                String bn_rxCui = hasIngredient.get(rxCui).first()    // SBDs have only one BN
                String bn_term = brandNames.get(bn_rxCui).getCodingFirstRep().getDisplay()
                Extension brandExtension = new Extension()
                        .setUrl("$FHIR_SERVER_URL/StructureDefinition/brand")
                        .setValue(new StringType(bn_term))
                med.addExtension(brandExtension)
                break
            case ['BPCK']:
                //med.setIsBrand(true)    // BPCKs do not have BNs
                break
            case ['SCD', 'GPCK']:
                //med.setIsBrand(false)
                break
        }

        switch (tty) {
            case ['SBD', 'SCD']:
                consistsOf.get(rxCui).each { String drugComponent_rxCui ->

                    MedicationIngredientComponent medIngredientComponent =
                            (MedicationIngredientComponent) getMedicationIngredientComponent(
                                    drugComponent_rxCui, false
                            )

                    med.addIngredient(medIngredientComponent)

                    MedicationKnowledge.MedicationKnowledgeIngredientComponent medKnowledgeIngredientComponent =
                            (MedicationKnowledge.MedicationKnowledgeIngredientComponent) getMedicationIngredientComponent(
                                    drugComponent_rxCui, true
                            )

                    medKnowledge.addIngredient(medKnowledgeIngredientComponent)
                }
                break
            case ['BPCK', 'GPCK']:
                contains.get(rxCui).each { String clinicalDrug_rxCui ->
                    consistsOf.get(clinicalDrug_rxCui).each { String drugComponent_rxCui ->
                        MedicationIngredientComponent medIngredientComponent =
                                (MedicationIngredientComponent) getMedicationIngredientComponent(
                                        drugComponent_rxCui, false
                                )

                        med.addIngredient(medIngredientComponent)

                        MedicationKnowledge.MedicationKnowledgeIngredientComponent medKnowledgeIngredientComponent =
                                (MedicationKnowledge.MedicationKnowledgeIngredientComponent) getMedicationIngredientComponent(
                                        drugComponent_rxCui, true
                                )

                        medKnowledge.addIngredient(medKnowledgeIngredientComponent)
                    }
                }
                break
        }

        med.setStatus(Medication.MedicationStatus.ACTIVE)
        med.setForm(doseForms.get(hasDoseForm.get(rxCui)))
        med.setCode(rxNormConcepts.get(rxCui))

        String medId = "rxNorm-$rxCui"    // use rxNorm-<rxCui> as resource ID
        med.setId(medId)
        medications.put(medId, med)

        Reference medReference = new Reference(med)
        medKnowledge.setAssociatedMedication(Collections.singletonList(medReference)) // link to Medication

        List<StringType> synonyms = rxNormSynonyms.get(rxCui).collect { new StringType (it) }
        medKnowledge.setSynonym(synonyms) // load RxNorm synonyms into MedicationKnowledge

        medKnowledge.setStatus("active")
        medKnowledge.setDoseForm(doseForms.get(hasDoseForm.get(rxCui)))
        medKnowledge.setCode(rxNormConcepts.get(rxCui)) // MedicationKnowledge uses the same identifier as Medication

        medKnowledge.setId(medId)    // use rxNorm-<rxCui> as resource ID
        medicationKnowledgeMap.put(medId, medKnowledge)

    }

    logStop()
}

Closure<IGenericClient> initiateConnection = {
    FhirContext ctxR4 = FhirContext.forR4()
    ctxR4.getRestfulClientFactory().setConnectTimeout(CONNECT_TIMEOUT_SEC * 1000)
    ctxR4.getRestfulClientFactory().setSocketTimeout(SOCKET_TIMEOUT_SEC * 1000)

    return ctxR4.newRestfulGenericClient(FHIR_SERVER_URL)
}

Closure loadBundleToServer = { IGenericClient newClient, Collection<? extends Resource> resources, String resourceType ->
    logStart("Loading $resourceType bundle (size: ${resources.size()}) to server")
    println()

    int total = resources.size()
    resources.collate(1000).each { batch ->
        Bundle input = new Bundle()

        batch.each {
            input.setType(BundleType.TRANSACTION)
            input.addEntry()
                    .setResource(it)
                    .getRequest()
                    .setUrlElement(new UriType("$resourceType/$it.id"))
                    .setMethod(HTTPVerb.PUT) // update resource if exists
        }

        Bundle response = newClient.transaction().withBundle(input).execute()

        assert response.getType() == BundleType.TRANSACTIONRESPONSE

        total = total - batch.size()
        println(total + " remaining")
    }

    logStop()
}

Closure writeSearchParameter = {
    SearchParameter brandSp = new SearchParameter()
    brandSp.addBase('Medication')
            .setCode('brand')
            .setType(Enumerations.SearchParamType.STRING)
            .setStatus(Enumerations.PublicationStatus.ACTIVE)
            .setXpathUsage(SearchParameter.XPathUsageType.NORMAL)
            .setExpression("Medication.extension('" + FHIR_SERVER_URL + "/StructureDefinition/brand')")
            .setTitle('Brand')

    parameters.add(brandSp)
}

readRxNormConceptsFile()
readRxNormRelationshipsFile()
readRxNormAttributesFile()
writeMedicationResources()
writeSearchParameter()


IGenericClient client = initiateConnection()
loadBundleToServer(client, parameters, 'SearchParameter')
loadBundleToServer(client, substances.values() as Collection<Substance>, 'Substance')
loadBundleToServer(client, medications.values() as Collection<Medication>, 'Medication')
loadBundleToServer(client, medicationKnowledgeMap.values() as Collection<MedicationKnowledge>, 'MedicationKnowledge')

