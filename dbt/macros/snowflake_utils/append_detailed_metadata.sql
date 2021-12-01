{% macro append_detailed_metadata(node) %}

    {% if execute %}            
        {{ log(node.identifier, info=True) }} --todo: figure out how to get the FQN from the node
        {{ log(tojson(graph.nodes['model.secret.' ~ node.identifier]), info=True) }}     

        -- TODO: Build the query to alter the description of the object with the metadata we extracted above. 
        {% set query = 'ALTER TABLE ' ~ node.identifier ~ ' SET COMMENT="' ~ tojson(graph.nodes['model.secret.' ~ node.identifier]) ~ '"";' %}       
        {% do run_query(query) %}        
    {% endif %}
    
{% endmacro %}