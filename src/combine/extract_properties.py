FLOAT_PROPERTIES = [
    "adult_mass_g",
    "brain_mass_g",
    "adult_body_length_mm",
    "adult_forearm_length_mm",
    "max_longevity_d",
    "maturity_d",
    "female_maturity_d",
    "male_maturity_d",
    "age_first_reproduction_d",
    "gestation_length_d",
    "teat_number_n",
    "litter_size_n",
    "litters_per_year_n",
    "interbirth_interval_d",
    "neonate_mass_g",
    "weaning_age_d",
    "weaning_mass_g",
    "generation_length_d",
    "dispersal_km",
    "density_n_km2",
    "home_range_km2",
    "social_group_n",
    "dphy_invertebrate",
    "dphy_vertebrate",
    "dphy_plant",
    "det_inv",
    "det_vend",
    "det_vect",
    "det_vfish",
    "det_vunk",
    "det_scav",
    "det_fruit",
    "det_nect",
    "det_seed",
    "det_plantother",
    "det_diet_breadth_n",
    "upper_elevation_m",
    "lower_elevation_m",
    "altitude_breadth_m",
    "habitat_breadth_n",
]

BOOL_PROPERTIES = [
    "hibernation_torpor",
    "freshwater",
    "marine",
    "terrestrial_non-volant",
    "terrestrial_volant",
    "island_dwelling",
    "disected_by_mountains",
    "glaciation",
]

CATEGORICAL_PROPERTIES = [
    "fossoriality",  # levels
    "trophic_level",  # 3 levels
    "foraging_stratum",  # 5 levels
    "activity_cycle",  # 3 levels
    "island_endemicity",  # 4 levels
]


def to_bool_property(data_point: str):
    if data_point == "NA":
        return None
    return bool(data_point)


def to_float_property(data_point: str):
    if data_point == "NA":
        return None
    return float(data_point)


def to_categorical_property(data_point: str):
    if data_point == "NA":
        return None
    return data_point


def extract_properties(row_data: dict) -> dict:
    properties = {
        key: to_categorical_property(value)
        for key, value in row_data.items()
        if key in CATEGORICAL_PROPERTIES
    }

    properties.update(
        {
            key: to_float_property(value)
            for key, value in row_data.items()
            if key in FLOAT_PROPERTIES
        }
    )
    properties.update(
        {
            key: to_bool_property(value)
            for key, value in row_data.items()
            if key in BOOL_PROPERTIES
        }
    )

    return properties
