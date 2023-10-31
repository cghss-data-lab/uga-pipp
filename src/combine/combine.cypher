UNWIND $Mapping as mapping
MERGE (tax:Taxon {name : mapping.iucn2020_binomial})
ON MATCH SET
    tax.adult_mass_g = mapping.adult_mass_g
    tax.brain_mass_g = mapping.brain_mass_g
    tax.adult_body_length_mm = mapping.adult_body_length_mm
    tax.adult_forearm_length_mm = mapping.adult_forearm_length_mm
    tax.max_longevity_d = mapping.max_longevity_d
    tax.maturity_d = mapping.maturity_d
    tax.female_maturity_d = mapping.female_maturity_d
    tax.male_maturity_d = mapping.male_maturity_d
    tax.age_first_reproduction_d = mapping.age_first_reproduction_d
    tax.gestation_length_d = mapping.gestation_length_d
    tax.teat_number_n = mapping.teat_number_n
    tax.litter_size_n = mapping.litter_size_n
    tax.litters_per_year_n = mapping.litters_per_year_n
    tax.interbirth_interval_d = mapping.interbirth_interval_d
    tax.neonate_mass_g = mapping.neonate_mass_g
    tax.weaning_age_d = mapping.weaning_age_d
    tax.weaning_mass_g = mapping.weaning_mass_g
    tax.generation_length_d = mapping.generation_length_d
    tax.dispersal_km = mapping.dispersal_km
    tax.density_n_km2 = mapping.density_n_km2
    tax.home_range_km2 = mapping.home_range_km2
    tax.social_group_n = mapping.social_group_n
    tax.dphy_invertebrate = mapping.dphy_invertebrate
    tax.dphy_vertebrate = mapping.dphy_vertebrate
    tax.dphy_plant = mapping.dphy_plant
    tax.det_inv = mapping.det_inv
    tax.det_vend = mapping.det_vend
    tax.det_vect = mapping.det_vect
    tax.det_vfish = mapping.det_vfish
    tax.det_vunk = mapping.det_vunk
    tax.det_scav = mapping.det_scav
    tax.det_fruit = mapping.det_fruit
    tax.det_nect = mapping.det_nect
    tax.det_seed = mapping.det_seed
    tax.det_plantother = mapping.det_plantother
    tax.det_diet_breadth_n = mapping.det_diet_breadth_n
    tax.upper_elevation_m = mapping.upper_elevation_m
    tax.lower_elevation_m = mapping.lower_elevation_m
    tax.altitude_breadth_m = mapping.altitude_breadth_m
    tax.habitat_breadth_n = mapping.habitat_breadth_n
    tax.hibernation_torpor = mapping.hibernation_torpor
    tax.freshwater = mapping.freshwater
    tax.marine = mapping.marine
    tax.terrestrial_non-volant = mapping.terrestrial_non-volant
    tax.terrestrial_volant = mapping.terrestrial_volant
    tax.island_dwelling = mapping.island_dwelling
    tax.disected_by_mountains = mapping.disected_by_mountains
    tax.glaciation = mapping.glaciation
    tax.fossoriality   = mapping.fossoriality  
    tax.trophic_level  = mapping.trophic_level 
    tax.foraging_stratum  = mapping.foraging_stratum 
    tax.activity_cycle   = mapping.activity_cycle  
    tax.island_endemicity = mapping.island_endemicity

FOREACH (realm in mapping.biogeographical_realm | 
    MERGE (geo:BioGeographicalRealm:Geography {name : realm })
    MERGE (tax)-[:INHABITS]->(geo)
)







