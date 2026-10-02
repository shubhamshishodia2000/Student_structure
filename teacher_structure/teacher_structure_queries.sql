-- Table 4.1. Includes only the requested display columns.
SELECT india_state_ut AS `India/State/UT`, total AS `Total Teacher`,
       foundational_preparatory AS `Foundational + Preparatory`,
       middle AS `Middle`, secondary AS `Secondary`
FROM udise_gold.teacher_structure_management
WHERE ac_year='2025-26' AND management='All Management'
ORDER BY india_state_ut;

-- Table 4.2. Change management to Government, Government Aided,
-- Private Unaided Recognized or Others for the corresponding report.
-- First column includes category 12 (pre-primary-only), matching your example.
SELECT india_state_ut AS `India/State/UT`, total AS `Total Teacher`,
       grades_1_5 AS `Foundational + Preparatory (1-5)`,
       grades_1_8 AS `Middle (1-8)`, grades_6_8 AS `Middle (6-8)`,
       grades_1_10 AS `Secondary (1-10)`, grades_6_10 AS `Secondary (6-10)`,
       grades_9_10 AS `Secondary (9-10)`, grades_1_12 AS `Secondary (1-12)`,
       grades_6_12 AS `Secondary (6-12)`, grades_9_12 AS `Secondary (9-12)`,
       grades_11_12 AS `Secondary (11-12)`
FROM udise_gold.teacher_structure_management
WHERE ac_year='2025-26' AND management='All Management'
ORDER BY india_state_ut;

-- Category-by-management report.
SELECT ac_year, india_state_ut, category, total,
       government, government_aided, private_unaided_recognized, others
FROM udise_gold.teacher_structure_category
WHERE ac_year='2025-26'
ORDER BY india_state_ut, category;

-- SCD2 revisions within the same academic year.
SELECT academic_year, udise_sch_code, male_tch, female_tch, transgen_tch,
       total_teachers, category, management_group,
       version_no, valid_from, valid_to, is_current, is_deleted, change_type
FROM udise_silver.fact_teacher_structure_history
WHERE academic_year='2025-26' AND udise_sch_code='02010100501'
ORDER BY version_no;
