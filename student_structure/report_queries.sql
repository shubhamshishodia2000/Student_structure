-- SQL for Superset / Power BI. Use one management scope per result.
SELECT ac_year, india_state_ut, total, foundational, preparatory, middle, secondary
FROM udise_gold.student_structure_summary
WHERE management = 'All Management'
ORDER BY ac_year, india_state_ut;

-- Available-source totals; do not add these to individual state rows.
SELECT * FROM udise_gold.student_structure_summary
WHERE india_state_ut = 'Available Source Total' AND management = 'All Management'
ORDER BY ac_year;
