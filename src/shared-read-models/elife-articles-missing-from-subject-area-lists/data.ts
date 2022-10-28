import * as RA from 'fp-ts/ReadonlyArray';
import { pipe } from 'fp-ts/function';
import * as Gid from '../../types/group-id';
import * as LID from '../../types/list-id';

export const elifeGroupId = Gid.fromValidatedString('b560187e-f2fb-4ff9-a861-a204f3fc0fb0');

// ts-unused-exports:disable-next-line
export const elifeSubjectAreaListIds = {
  biochemistryAndChemicalBiologyListId: '3792ee73-6a7d-4c54-b6ee-0abc18cb8bc4',
  bioengineeringListId: 'b2b55ddd-c0f2-4406-b304-b744af989e72',
  cancerBiologyListId: '977cec9b-7ff6-4cf5-a487-30f0cc544cdb',
  cellBiologyListId: 'cb15ef21-944d-44d6-b415-a3d8951e9e8b',
  computationalAndSystemsBiologyListId: 'c9efbf2e-8d20-4a9a-b407-c25d185b4939',
  developmentalBiologyListId: '1008fbbe-9d14-4737-808f-4170640df9cb',
  ecologyListId: 'a9f35fb7-c2fe-4fde-af39-f7c79ea0a497',
  epidemiologyListId: '0453b3c1-d58e-429f-8c1e-588ccc646113',
  evolutionaryBiologyListId: '5146099b-22e0-4589-9f16-10586e08ca4b',
  geneticsAndGenomicsListId: '890bf35a-c3da-413a-8cdb-864b7ce91a51',
  immunologyAndInflammationListId: 'b4acc6f3-bf15-4add-ab1f-bc72a8a3da7f',
  medicineListId: 'c7237468-aac1-4132-9598-06e9ed68f31d',
  microbiologyAndInfectiousDiseaseListId: 'db62bf5b-bcd4-42eb-bd99-e7a37283041d',
  molecularBiologyListId: '708b4836-0adf-4326-844f-fdf8ef816402',
  neuroscienceListId: '3253c905-8083-4f3d-9e1f-0a8085e64ee5',
  pharmacologyAndToxicologyListId: '84577aec-a4ab-4c61-8c2e-b799a3918350',
  physiologyListId: '57a4fa09-d9f5-466d-8038-ea9d29603aef',
  plantBiologyListId: '205415a7-b409-4ded-ada2-3116c953c4c2',
  scientificCommunicationAndEducationListId: 'd3d30687-62ee-4bb6-8723-f8d49dab7882',
  structuralBiologyAndMolecularBiophysicsListId: 'a059f20a-366d-4790-b1f2-03bfb9b915b6',
  syntheticBiologyListId: 'c743bc3d-955a-4e97-b897-5e423ef0d3bc',
  zoologyListId: '86a14824-8a48-4194-b75a-efbca28b90ae',
};

export const mappingOfBiorxivAndMedrxivSubjectAreasToELifeLists: Record<string, string> = {
  // biorxiv
  'animal behavior and cognition': elifeSubjectAreaListIds.neuroscienceListId,
  biochemistry: elifeSubjectAreaListIds.biochemistryAndChemicalBiologyListId,
  bioengineering: elifeSubjectAreaListIds.bioengineeringListId,
  bioinformatics: elifeSubjectAreaListIds.computationalAndSystemsBiologyListId,
  biophysics: elifeSubjectAreaListIds.structuralBiologyAndMolecularBiophysicsListId,
  'cancer biology': elifeSubjectAreaListIds.cancerBiologyListId,
  'cell biology': elifeSubjectAreaListIds.cellBiologyListId,
  'developmental biology': elifeSubjectAreaListIds.developmentalBiologyListId,
  ecology: elifeSubjectAreaListIds.ecologyListId,
  // epidemiology: elifeSubjectAreaListIds.epidemiologyListId,
  'evolutionary biology': elifeSubjectAreaListIds.evolutionaryBiologyListId,
  genetics: elifeSubjectAreaListIds.geneticsAndGenomicsListId,
  genomics: elifeSubjectAreaListIds.geneticsAndGenomicsListId,
  immunology: elifeSubjectAreaListIds.immunologyAndInflammationListId,
  microbiology: elifeSubjectAreaListIds.microbiologyAndInfectiousDiseaseListId,
  'molecular biology': elifeSubjectAreaListIds.molecularBiologyListId,
  neuroscience: elifeSubjectAreaListIds.neuroscienceListId,
  paleontology: elifeSubjectAreaListIds.evolutionaryBiologyListId,
  'pharmacology and toxicology': elifeSubjectAreaListIds.pharmacologyAndToxicologyListId,
  physiology: elifeSubjectAreaListIds.physiologyListId,
  'plant biology': elifeSubjectAreaListIds.plantBiologyListId,
  'scientific communication and education': elifeSubjectAreaListIds.scientificCommunicationAndEducationListId,
  'synthetic biology': elifeSubjectAreaListIds.syntheticBiologyListId,
  'systems biology': elifeSubjectAreaListIds.computationalAndSystemsBiologyListId,
  zoology: elifeSubjectAreaListIds.zoologyListId,
  // medrxiv
  'addiction medicine': elifeSubjectAreaListIds.medicineListId,
  'allergy and immunology': elifeSubjectAreaListIds.immunologyAndInflammationListId,
  anesthesia: elifeSubjectAreaListIds.medicineListId,
  'cardiovascular medicine': elifeSubjectAreaListIds.medicineListId,
  'dentistry and oral medicine': elifeSubjectAreaListIds.medicineListId,
  dermatology: elifeSubjectAreaListIds.medicineListId,
  'emergency medicine': elifeSubjectAreaListIds.medicineListId,
  endocrinology: elifeSubjectAreaListIds.medicineListId,
  epidemiology: elifeSubjectAreaListIds.epidemiologyListId,
  'forensic medicine': elifeSubjectAreaListIds.medicineListId,
  gastroenterology: elifeSubjectAreaListIds.medicineListId,
  'genetic and genomic medicine': elifeSubjectAreaListIds.medicineListId,
  'geriatric medicine': elifeSubjectAreaListIds.medicineListId,
  'health economics': elifeSubjectAreaListIds.medicineListId,
  'health informatics': elifeSubjectAreaListIds.medicineListId,
  'health policy': elifeSubjectAreaListIds.medicineListId,
  'health systems and quality improvement': elifeSubjectAreaListIds.medicineListId,
  hematology: elifeSubjectAreaListIds.medicineListId,
  'hiv aids': elifeSubjectAreaListIds.medicineListId,
  'infectious diseases': elifeSubjectAreaListIds.medicineListId,
  'intensive care and critical care medicine': elifeSubjectAreaListIds.medicineListId,
  'medical education': elifeSubjectAreaListIds.medicineListId,
  'medical ethics': elifeSubjectAreaListIds.medicineListId,
  nephrology: elifeSubjectAreaListIds.medicineListId,
  neurology: elifeSubjectAreaListIds.neuroscienceListId,
  nursing: elifeSubjectAreaListIds.medicineListId,
  nutrition: elifeSubjectAreaListIds.medicineListId,
  'obstetrics and gynecology': elifeSubjectAreaListIds.medicineListId,
  'occupational and environmental health': elifeSubjectAreaListIds.medicineListId,
  oncology: elifeSubjectAreaListIds.cancerBiologyListId,
  ophthalmology: elifeSubjectAreaListIds.medicineListId,
  orthopedics: elifeSubjectAreaListIds.medicineListId,
  otolaryngology: elifeSubjectAreaListIds.medicineListId,
  'pain medicine': elifeSubjectAreaListIds.medicineListId,
  'palliative medicine': elifeSubjectAreaListIds.medicineListId,
  pathology: elifeSubjectAreaListIds.medicineListId,
  pediatrics: elifeSubjectAreaListIds.medicineListId,
  'pharmacology and therapeutics': elifeSubjectAreaListIds.medicineListId,
  'primary care research': elifeSubjectAreaListIds.medicineListId,
  'psychiatry and clinical psychology': elifeSubjectAreaListIds.medicineListId,
  'public and global health': elifeSubjectAreaListIds.epidemiologyListId,
  'radiology and imaging': elifeSubjectAreaListIds.medicineListId,
  'rehabilitation medicine and physical therapy': elifeSubjectAreaListIds.medicineListId,
  'respiratory medicine': elifeSubjectAreaListIds.medicineListId,
  rheumatology: elifeSubjectAreaListIds.medicineListId,
  'sexual and reproductive health': elifeSubjectAreaListIds.medicineListId,
  'sports medicine': elifeSubjectAreaListIds.medicineListId,
  surgery: elifeSubjectAreaListIds.medicineListId,
  toxicology: elifeSubjectAreaListIds.medicineListId,
  transplantation: elifeSubjectAreaListIds.medicineListId,
  urology: elifeSubjectAreaListIds.medicineListId,
};

export const elifeSubjectAreaLists = pipe(
  Object.values(elifeSubjectAreaListIds),
  RA.map(LID.fromValidatedString),
);
