package main.groovy

import ca.uhn.fhir.context.FhirContext
import ca.uhn.fhir.rest.client.api.IGenericClient
import com.google.common.base.Stopwatch
import com.google.common.collect.HashBasedTable
import com.google.common.collect.HashMultimap
import com.google.common.collect.SetMultimap
import com.google.common.collect.Table
import org.hl7.fhir.r4.model.*
import org.hl7.fhir.r4.model.Bundle.BundleType
import org.hl7.fhir.r4.model.Bundle.HTTPVerb
import org.hl7.fhir.r4.model.Medication.MedicationIngredientComponent

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths

Stopwatch watch = Stopwatch.createStarted()

boolean isForLocalTesting = true

String outputFolder = System.getProperty("user.dir") + File.separator + "target"

int CONNECT_TIMEOUT_SEC = 10
int SOCKET_TIMEOUT_SEC = 180

String FHIR_SERVER_URL = 'http://localhost:8080/hapi-fhir-jpaserver/fhir/'
String RXNORM_SYSTEM = 'http://www.nlm.nih.gov/research/umls/rxnorm'
String RXNORM_VERSION = '02042019'
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
SetMultimap<String, String> hasIngredient = HashMultimap.create()
SetMultimap<String, String> consistsOf = HashMultimap.create()
SetMultimap<String, String> contains = HashMultimap.create()
SetMultimap<String, String> isa = HashMultimap.create()
SetMultimap<String, String> hasDoseFormGroup = HashMultimap.create()

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

// data structures to store RxNorm units of measure
Set<String> unitsOfMeasure = []

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
                case ['SCD', 'SBD', 'GPCK', 'BPCK', 'SCDF', 'SBDF']: // RXCUI, STR
                    CodeableConcept concept = new CodeableConcept()
                            .addCoding(new Coding(RXNORM_SYSTEM, tokens.get(0), tokens.get(14)))
                    rxNormConcepts.put(tokens.get(0), concept)
                    rxNormTty.put(tokens.get(0), tokens.get(12))
                    break
            }

        }

        // only consider non-suppressed synonyms and exclude DrugBank terms
        if (tokens.get(16) == "N" || tokens.get(16) == "") {
            if (tokens.get(11) != "DRUGBANK") {
                switch (tokens.get(12)) {
                    case ['SY', 'PSN', 'PT']:// RXCUI, STR
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
                case 'has_doseformgroup':
                    hasDoseFormGroup.put(tokens.get(4), tokens.get(0))
                    break
                case 'contains':
                    contains.put(tokens.get(4), tokens.get(0))
                    break
                case 'isa':
                    isa.put(tokens.get(4), tokens.get(0))
                    break
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
                // New basis of strength attributes since September 2018 release
                // https://www.nlm.nih.gov/pubs/techbull/so18/brief/so18_rxnorm_boss.html
                case [
                        'RXN_BOSS_STRENGTH_NUM_VALUE',
                        'RXN_BOSS_STRENGTH_NUM_UNIT',
                        'RXN_BOSS_STRENGTH_DENOM_VALUE',
                        'RXN_BOSS_STRENGTH_DENOM_UNIT'
                ]: // RXCUI, ATN, ATV
                    attributes.put(tokens.get(0), attribName, tokens.get(10))
                    break
                case 'RXN_STRENGTH': // RXCUI, ATN, ATV
                    attributes.put(tokens.get(0), attribName, tokens.get(10))
                    break
            }
        }
    }

    cpcRxnSat.close()
    logStop()
}

Closure<Ratio> getAmount = { Map<String, String> scdcAttributes ->
    Ratio amount = new Ratio()

    if (scdcAttributes) {
        if (scdcAttributes.get('RXN_BOSS_STRENGTH_DENOM_UNIT') &&
                scdcAttributes.get('RXN_BOSS_STRENGTH_DENOM_VALUE') &&
                scdcAttributes.get('RXN_BOSS_STRENGTH_NUM_UNIT') &&
                scdcAttributes.get('RXN_BOSS_STRENGTH_NUM_VALUE')) {

            String denominatorUnit = scdcAttributes.get('RXN_BOSS_STRENGTH_DENOM_UNIT')
            Double denominatorValue = scdcAttributes.get('RXN_BOSS_STRENGTH_DENOM_VALUE').toDouble()
            String numeratorUnit = scdcAttributes.get('RXN_BOSS_STRENGTH_NUM_UNIT')
            Double numeratorValue = scdcAttributes.get('RXN_BOSS_STRENGTH_NUM_VALUE').toDouble()

            amount.setNumerator(new Quantity().setValue(numeratorValue).setUnit(numeratorUnit))
                    .setDenominator(new Quantity().setValue(denominatorValue).setUnit(denominatorUnit))

        } else if (scdcAttributes.get('RXN_STRENGTH')) {

            String strength = scdcAttributes.get('RXN_STRENGTH')

            Double numeratorValue = strength.split(" ")[0].toDouble()

            List<String> unitDenominator = strength.split(" ")[1].split("/").toList()

            String numeratorUnit = unitDenominator.get(0)

            unitsOfMeasure.add(numeratorUnit)

            if (unitDenominator.size() == 2) {
                String denominatorUnit = unitDenominator.get(1)
                Double denominatorValue = 1

                unitsOfMeasure.add(denominatorUnit)

                amount.setNumerator(new Quantity().setValue(numeratorValue).setUnit(numeratorUnit))
                        .setDenominator(new Quantity().setValue(denominatorValue).setUnit(denominatorUnit))
            } else {

                String denominatorUnit = "1"
                Double denominatorValue = 1

                amount.setNumerator(new Quantity().setValue(numeratorValue).setUnit(numeratorUnit))
                        .setDenominator(new Quantity().setValue(denominatorValue).setUnit(denominatorUnit))
            }
        }
    }

    return amount
}

Closure<BackboneElement> getMedicationIngredientComponent = { String scdc_rxCui, boolean forKnowledge ->
    String ing_rxCui = hasIngredient.get(scdc_rxCui).first()    // assume each component has only one ingredient

    CodeableConcept ingredient = ingredients.get(ing_rxCui)

    BackboneElement component

    if (ingredient) {
        if (forKnowledge) {
            component = new MedicationKnowledge.MedicationKnowledgeIngredientComponent()
        } else {
            component = new MedicationIngredientComponent()
        }

        String substanceId = "rxNorm-$ing_rxCui"    // use rxNorm-<rxCui> as resource ID

        Substance substance = substances.get(substanceId)

        if (component instanceof MedicationIngredientComponent) {
            component = (MedicationIngredientComponent) component

            Reference substanceReference = new Reference(substance)
            component.setItem(substanceReference)

            Map<String, String> scdcAttributes = attributes.row(scdc_rxCui)

            component.setStrength(getAmount(scdcAttributes))

            component.setIsActive(true)

        } else if (component instanceof MedicationKnowledge.MedicationKnowledgeIngredientComponent) {
            component = (MedicationKnowledge.MedicationKnowledgeIngredientComponent) component

            Reference substanceReference = new Reference(substance)
            component.setItem(substanceReference)

            Map<String, String> scdcAttributes = attributes.row(scdc_rxCui)

            component.setStrength(getAmount(scdcAttributes))

            component.setIsActive(true)
        }

        return component

    }
}

Closure writeSubstanceResources = {
    logStart('Writing FHIR Substance resources')

    ingredients.each { String ing_rxCui, CodeableConcept concept ->

        Substance substance = new Substance()

        substance.setStatus(Substance.FHIRSubstanceStatus.ACTIVE)
        substance.setCode(concept)

        List<StringType> synonyms = rxNormSynonyms.get(ing_rxCui).collect { new StringType(it) }.toList()

        String synonymUrl = FHIR_SERVER_URL + "StructureDefinition/synonym"

        synonyms.each {
            Extension synonymExtension = new Extension()
                    .setUrl(synonymUrl)
                    .setValue(it)
            substance.addExtension(synonymExtension)
        }

        String substanceId = "rxNorm-$ing_rxCui"    // use rxNorm-<rxCui> as resource ID
        substance.setId(substanceId)

        substances.put(substanceId, substance)
    }

    logStop()
}

Closure writeMedicationResources = {
    logStart('Writing FHIR Medication resources')

    rxNormConcepts.keySet().each { rxCui ->

        Medication med = new Medication()
        MedicationKnowledge medKnowledge = new MedicationKnowledge()

        String tty = rxNormTty.get(rxCui)

        switch (tty) {
            case ['SBD']:
                String bn_rxCui = hasIngredient.get(rxCui).first()    // SBDs have only one BN
                String bn_term = brandNames.get(bn_rxCui).getCodingFirstRep().getDisplay()

                String brandUrl = FHIR_SERVER_URL + "StructureDefinition/brand"

                Extension brandExtension = new Extension()
                        .setUrl(brandUrl)
                        .setValue(new StringType(bn_term))
                med.addExtension(brandExtension)
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

        String doseFormId = hasDoseForm.get(rxCui) ?: hasDoseFormGroup.get(rxCui)

        if (doseFormId) {
            med.setForm(doseForms.get(doseFormId))
        }

        med.setCode(rxNormConcepts.get(rxCui))

        String medId = "rxNorm-$rxCui"    // use rxNorm-<rxCui> as resource ID
        med.setId(medId)
        medications.put(medId, med)


        // store associated parent concepts (SCDF/SBDF) in MedicationKnowledge
        Set<String> parentIds = isa.get(rxCui)

        if (parentIds) {
            medKnowledge.setAssociatedMedication(
                    parentIds.collect { new Reference("Medication/rxNorm-$it") }
            )
        }

        List<StringType> synonyms = rxNormSynonyms.get(rxCui)
                .unique { s1, s2 -> s1.compareToIgnoreCase(s2) }
                .collect { new StringType(it) }

        if (synonyms) {
            medKnowledge.setSynonym(synonyms) // load RxNorm synonyms into MedicationKnowledge
        }

        medKnowledge.setStatus("active")
        medKnowledge.setDoseForm(doseForms.get(hasDoseForm.get(rxCui)))
        medKnowledge.setCode(rxNormConcepts.get(rxCui)) // MedicationKnowledge uses the same identifier as Medication

        medKnowledge.setId(medId)    // use rxNorm-<rxCui> as resource ID
        medicationKnowledgeMap.put(medId, medKnowledge)

    }

    logStop()
}

Closure<IGenericClient> initiateConnection = { FhirContext context ->
    context.getRestfulClientFactory().setConnectTimeout(CONNECT_TIMEOUT_SEC * 1000)
    context.getRestfulClientFactory().setSocketTimeout(SOCKET_TIMEOUT_SEC * 1000)

    return context.newRestfulGenericClient(FHIR_SERVER_URL)
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
    logStart("Writing FHIR SearchParameter resources")

    SearchParameter brandSp = new SearchParameter()
    brandSp.addBase('Medication')
            .setCode('brand')
            .setType(Enumerations.SearchParamType.STRING)
            .setStatus(Enumerations.PublicationStatus.ACTIVE)
            .setXpathUsage(SearchParameter.XPathUsageType.NORMAL)
            .setExpression("Medication.extension('" + FHIR_SERVER_URL + "StructureDefinition/brand')")
            .setTitle('Brand')
            .setId('brand')

    parameters.add(brandSp)

    SearchParameter synonymSp = new SearchParameter()
    synonymSp.addBase('Substance')
            .setCode('synonym')
            .setType(Enumerations.SearchParamType.STRING)
            .setStatus(Enumerations.PublicationStatus.ACTIVE)
            .setXpathUsage(SearchParameter.XPathUsageType.NORMAL)
            .setExpression("Substance.extension('" + FHIR_SERVER_URL + "StructureDefinition/synonym')")
            .setTitle('Synonym')
            .setId('synonym')

    parameters.add(synonymSp)

    logStop()
}

Closure writeBundleToFile = { FhirContext context, String folder, Collection<? extends Resource> resources, String resourceType ->
    logStart("Loading $resourceType bundle (size: ${resources.size()}) to server")

    Path path = Paths.get(folder + File.separator + resourceType + ".json")
    BufferedWriter writer = Files.newBufferedWriter(path)

    Bundle output = new Bundle()

    resources.each {
        output.addEntry().setResource(it)
    }

    context.newJsonParser().setPrettyPrint(true).encodeResourceToWriter(output, writer)

    writer.flush()
    writer.close()

    logStop()
}

readRxNormConceptsFile()
readRxNormRelationshipsFile()
readRxNormAttributesFile()
writeSubstanceResources()
writeMedicationResources()
writeSearchParameter()

FhirContext context = FhirContext.forR4()

if (isForLocalTesting) {

    writeBundleToFile(context, outputFolder, substances.values() as Collection<Substance>, 'Substance')
    writeBundleToFile(context, outputFolder, medications.values() as Collection<Substance>, 'Medication')
    writeBundleToFile(context, outputFolder, medicationKnowledgeMap.values() as Collection<Substance>, 'MedicationKnowledge')

} else {

    IGenericClient client = initiateConnection(context)
    loadBundleToServer(client, parameters, 'SearchParameter')
    loadBundleToServer(client, substances.values() as Collection<Substance>, 'Substance')
    loadBundleToServer(client, medications.values() as Collection<Medication>, 'Medication')
    loadBundleToServer(client, medicationKnowledgeMap.values() as Collection<MedicationKnowledge>, 'MedicationKnowledge')

}

println("Units of measure detected: " + unitsOfMeasure.unique().sort())

println("Total time taken " + watch.stop().toString())