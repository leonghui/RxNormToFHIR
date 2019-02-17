package main.groovy

import ca.uhn.fhir.context.FhirContext
import ca.uhn.fhir.rest.client.api.IGenericClient
import com.google.common.base.Stopwatch
import com.google.common.collect.HashBasedTable
import com.google.common.collect.HashMultimap
import com.google.common.collect.Multimap
import com.google.common.collect.Table
import org.hl7.fhir.dstu3.model.*
import org.hl7.fhir.dstu3.model.Bundle.BundleType
import org.hl7.fhir.dstu3.model.Bundle.HTTPVerb
import org.hl7.fhir.dstu3.model.Medication.MedicationIngredientComponent

Stopwatch watch = Stopwatch.createStarted()

int CONNECT_TIMEOUT_SEC = 10
int SOCKET_TIMEOUT_SEC = 90

String FHIR_SERVER_URL = 'http://<server>:<port>/baseDstu3'
String RXNORM_SYSTEM = 'http://www.nlm.nih.gov/research/umls/rxnorm'
String RXNORM_VERSION = '12032018'
String RXNORM_FOLDER_NAME = "RxNorm_full_prescribe_$RXNORM_VERSION"

// rxTerms release
String RXTERMS_VERSION = '201812'
String RXTERMS_FOLDER_NAME = "RxTerms$RXTERMS_VERSION"

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

// data structures to store FHIR resources
// FHIR ID, Resource
Map<String, Medication> medications = [:]
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

		List<String> tokens = line.split(/\|/)

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
				break
		}
	}

	cpcRxnConso.close()
	logStop()
}

Closure readRxNormRelationshipsFile = {
	logStart('Reading RxNorm relationships file')
	FileReader cpcRxnRel = new FileReader("src/main/resources/$RXNORM_FOLDER_NAME/rrf/RXNREL.RRF")

	cpcRxnRel.eachLine { String line, int number ->

		List<String> tokens = line.split(/\|/)

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

	cpcRxnRel.close()
	logStop()
}

Closure readRxNormAttributesFile = {
	logStart('Reading RxNorm attributes file')
	FileReader cpcRxnSat = new FileReader("src/main/resources/$RXNORM_FOLDER_NAME/rrf/RXNSAT.RRF")

	cpcRxnSat.eachLine { String line, int number ->

		List<String> tokens = line.split(/\|/)

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

	cpcRxnSat.close()
	logStop()
}

Closure<MedicationIngredientComponent> getMedicationIngredientComponent = { String scdc_rxCui ->
	String ing_rxCui = hasIngredient.get(scdc_rxCui).first()    // assume each component has only one ingredient

	CodeableConcept ingredient = ingredients.get(ing_rxCui)

	if (ingredient) {
		MedicationIngredientComponent component = new MedicationIngredientComponent()

		Substance substance = new Substance()

		substance.setStatus(Substance.FHIRSubstanceStatus.ACTIVE)
		substance.setCode(ingredients.get(ing_rxCui))

		String substanceId = "rxNorm-$ing_rxCui"    // use rxNorm-<rxCui> as resource ID
		substance.setId(substanceId)

		substances.put(substanceId, substance)

		// BUG: setItem() in HAPI-FHIR 3.6.0 does not accept Reference as value
		// Reference substanceReference = new Reference ('Substance' + '/' + substanceId)
		// component.setItem(substanceReference)
		component.setItem(ingredients.get(ing_rxCui))

		Map<String, String> scdcAttributes = attributes.row(scdc_rxCui)

		if (scdcAttributes) {
			String denominatorUnit = scdcAttributes.get('RXN_BOSS_STRENGTH_DENOM_UNIT')
			Double denominatorValue = scdcAttributes.get('RXN_BOSS_STRENGTH_DENOM_VALUE').toDouble()
			String numeratorUnit = scdcAttributes.get('RXN_BOSS_STRENGTH_NUM_UNIT')
			Double numeratorValue = scdcAttributes.get('RXN_BOSS_STRENGTH_NUM_VALUE').toDouble()

			Ratio amount = new Ratio()
					.setNumerator(new Quantity().setValue(numeratorValue).setUnit(numeratorUnit))
					.setDenominator(new Quantity().setValue(denominatorValue).setUnit(denominatorUnit))

			component.setAmount(amount)
		}

		component.setIsActive(true)

		return component

	}
}

Closure readRxTermsFile = {
	logStart('Reading RxTerms file')
	FileReader rxTerms = new FileReader("src/main/resources/$RXTERMS_FOLDER_NAME/RxTerms${RXTERMS_VERSION}.txt")

	rxTerms.eachLine { String line, int number ->
		if (number == 1) return

		/*
		 * 0	RXCUI
		 * 1	GENERIC_RXCUI
		 * 2	TTY
		 * 3	FULL_NAME
		 * 4	RXN_DOSE_FORM
		 * 5	FULL_GENERIC_NAME
		 * 6	BRAND_NAME
		 * 7	DISPLAY_NAME
		 * 8	ROUTE
		 * 9	NEW_DOSE_FORM
		 * 10	STRENGTH
		 * 11	SUPPRESS_FOR
		 * 12	DISPLAY_NAME_SYNONYM
		 * 13	IS_RETIRED
		 * 14	SXDG_RXCUI
		 * 15	SXDG_TTY
		 * 16	SXDG_NAME
		 * 17	PSN
		 */
		List<String> tokens = line.split(/\|/)

		String rxCui = tokens.get(0)
		String tty = tokens.get(2)

		Medication med = new Medication()

		switch (tty) {
			case ['SBD']:
				med.setIsBrand(true)
				String bn_rxCui = hasIngredient.get(rxCui).first()	// SBDs have only one BN
				String bn_term = brandNames.get(bn_rxCui).getCodingFirstRep().getDisplay()
				Extension brandExtension = new Extension()
						.setUrl("$FHIR_SERVER_URL/StructureDefinition/brand")
						.setValue(new StringType(bn_term))
				med.addExtension(brandExtension)
				break
			case ['BPCK']:
				med.setIsBrand(true)	// BPCKs do not have BNs
				break
			case ['SCD', 'GPCK']:
				med.setIsBrand(false)
				break
		}

		switch (tty) {
			case ['SBD', 'SCD']:
				consistsOf.get(rxCui).each { String drugComponent_rxCui ->
					med.addIngredient(getMedicationIngredientComponent(drugComponent_rxCui))
				}
				break
			case ['BPCK', 'GPCK']:
				contains.get(rxCui).each { String clinicalDrug_rxCui ->
					consistsOf.get(clinicalDrug_rxCui).each { String drugComponent_rxCui ->
						med.addIngredient(getMedicationIngredientComponent(drugComponent_rxCui))
					}
				}
				break
		}

		med.setStatus(Medication.MedicationStatus.ACTIVE)
		med.setForm(doseForms.get(hasDoseForm.get(rxCui)))
		med.setCode(rxNormConcepts.get(rxCui))
		String medId = "rxNorm-$rxCui"  // use rxNorm-<rxCui> as resource ID
		med.setId(medId)
		medications.put(medId, med)

	}

	rxTerms.close()
	logStop()
}

Closure<IGenericClient> initiateConnection = {
	FhirContext ctxDstu3 = FhirContext.forDstu3()
	ctxDstu3.getRestfulClientFactory().setConnectTimeout(CONNECT_TIMEOUT_SEC * 1000)
	ctxDstu3.getRestfulClientFactory().setSocketTimeout(SOCKET_TIMEOUT_SEC * 1000)

	return ctxDstu3.newRestfulGenericClient(FHIR_SERVER_URL)
}

Closure loadBundleToServer = { IGenericClient newClient, Collection<? extends Resource> resources, String resourceType ->
	logStart("Loading $resourceType bundle to server")

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
readRxTermsFile()
writeSearchParameter()

IGenericClient client = initiateConnection()
loadBundleToServer(client, parameters, 'SearchParameter')
loadBundleToServer(client, medications.values() as Collection<Medication>, 'Medication')
loadBundleToServer(client, substances.values() as Collection<Substance>, 'Substance')
