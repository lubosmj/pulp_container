SIGNATURE_SCHEMA = '''{
	"$schema": "https://json-schema.org/draft/2019-09/schema",
    "$id": "https://example.com/product.schema.json",
    "title": "Signature",
    "description": "JSON signature payload",
    "type": "object",
    "properties": {
        "critical": {
            "type": "object",
            "properties": {
                "type": {
                	"type": "string",
                    "const": "atomic container signature",
                },
                "image": {
                	"type": "object",
                    "properties": {
                    	"docker-image-digest": {
                        	"type": "string"
                        }
                    }
                },
                "identity": {
                	"type": "object",
                    "properties": {
                    	"docker-reference": {
                        	"type": "string"
                        }
                    }
                }
            }
    	},
        "optional": {
        	"type": "object",
            "properties": {
            	"creator": {
                	"type": "string"
                },
                "timestamp": {
                	"type": "number",
                    "minimum": 0,
                }
            }
        }
    },
    "required": ["critical"],
    "additionalProperties": false
}'''
