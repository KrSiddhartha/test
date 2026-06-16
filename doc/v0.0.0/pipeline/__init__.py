"""Ported schema-creation pipeline for the Schema Intelligence service (v0.0.0).

render -> extract (generic nodes) -> induce (structural) -> concepts (pure dictionary) ->
schema_build (-> contract SchemaNode tree + fieldProfiles). guides loads guideDoc/suggestedSchema
hints. Self-contained: nothing here imports the experimental schema_infer/ tree.
"""
