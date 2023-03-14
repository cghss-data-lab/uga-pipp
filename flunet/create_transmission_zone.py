from loguru import logger


def create_transmission_zone(zone, session):
    """
    Creates a TransmissionZone node and links it to the GeoNames system.
    
    Args:
        zone (str): Name of the transmission zone.
        session (neo4j.Session): Neo4j session object.

    Returns:
        None
    """
    query = """
        CREATE (n:TransmissionZone:Geo {name: $name})
        RETURN n
    """
    params = {"name": zone}
    try:
        result = session.run(query, params)
        logger.info(f"Created transmission zone node ({zone})")
    except Exception as e:
        logger.error(f"Error creating transmission zone node ({zone}): {e}")
