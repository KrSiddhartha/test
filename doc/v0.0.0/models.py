"""Pydantic models mirroring API_DOCUMENTATION.md exactly.

SchemaNode is recursive. Per the agreed convention a repeating table is an OBJECT node with
data_type=ARRAY whose children define the row; a scalar is a FIELD leaf carrying fieldProfileRef.
"""
from typing import List, Optional

try:
    from typing import Literal
except ImportError:  # pragma: no cover
    Literal = None

from pydantic import BaseModel, Field

NodeType = str   # "OBJECT" | "FIELD"
DataType = str   # "OBJECT" | "STRING" | "INTEGER" | "BOOLEAN" | "ARRAY"


class SchemaNode(BaseModel):
    field_name: str
    fieldProfileRef: Optional[str] = None        # leaf (FIELD) only — "FIELD#<parent>.<field_name>"
    label: str
    description: str
    node_type: NodeType                          # OBJECT (container) | FIELD (leaf)
    data_type: DataType                          # OBJECT for containers; STRING/INTEGER/BOOLEAN/ARRAY for leaves/repeating
    children: List["SchemaNode"] = Field(default_factory=list)


# resolve the forward reference (pydantic v2: model_rebuild; v1: update_forward_refs)
try:
    SchemaNode.model_rebuild()
except AttributeError:  # pragma: no cover
    SchemaNode.update_forward_refs()


class SourceMapping(BaseModel):
    sourcePath: str
    targetPath: str
    confidenceScore: Optional[float] = None      # schema-generation responses
    confidence: Optional[float] = None           # mapper-agent responses (contract's known inconsistency)


class FieldProfile(BaseModel):
    sourceMapping: SourceMapping


class Metadata(BaseModel):
    profile_id: int


class Profile(BaseModel):
    metadata: Metadata
    schemaInfo: List[SchemaNode] = Field(default_factory=list)
    fieldProfiles: List[FieldProfile] = Field(default_factory=list)


# ---- requests ----
class SchemaGenerationRequest(BaseModel):
    metadata: Metadata
    sampleFiles: List[str]
    guideDoc: List[str] = Field(default_factory=list)
    suggestedSchema: Optional[str] = None


class MapperAgentRequest(BaseModel):
    metadata: Metadata
    schemaInfo: List[SchemaNode]


# ---- response envelope (also the webhook payload body) ----
class ApiResponse(BaseModel):
    code: int
    message: str
    status: str                                  # "success" | "error" | "accepted"
    profile: Optional[Profile] = None
