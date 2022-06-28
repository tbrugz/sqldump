<?xml version="1.0"?>
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
     xmlns:y='http://www.yworks.com/xml/graphml' xmlns:g="http://graphml.graphdrawing.org/xmlns" version="1.0">

  <xsl:output indent="yes"/>
  
  <xsl:template match="/Schema">
    <g:graphml xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
               xsi:schemaLocation="http://graphml.graphdrawing.org/xmlns http://www.yworks.com/xml/schema/graphml/1.1/ygraphml.xsd">
      <g:key id="d0" for="node" yfiles.type="nodegraphics"/>
      <g:key id="d1" for="edge" yfiles.type="edgegraphics"/>
      <g:key id="d2" for="graph" yfiles.type="postprocessors"/>
      <g:graph id="G" edgedefault="directed">

        <!-- xsl:apply-templates select="target"/ -->
        <xsl:apply-templates select="child::*" />
        
        <!-- xsl:apply-templates select="target" mode="nodes"/>
        <xsl:apply-templates select="target[@depends]" mode="edges"/-->

      </g:graph>
    </g:graphml>
  
  </xsl:template>

  <xsl:template match="Cube">
    <xsl:element name="g:node">
      <xsl:attribute name="id">
        <xsl:value-of select="@name"/>
      </xsl:attribute>
      <g:data key="d0">
        <y:ShapeNode>
          <y:Geometry height="50.0" width="120.0"/>
          <y:Fill color="#CCCCFF"/>
          <y:NodeLabel>
            <xsl:value-of select="@name"/>
            <xsl:apply-templates select="child::Measure" />
            <xsl:apply-templates select="child::CalculatedMember" />
          </y:NodeLabel>
        </y:ShapeNode>
      </g:data>
    </xsl:element>
    <xsl:apply-templates select="child::Dimension" />
    <xsl:apply-templates select="child::DimensionUsage" />
    <!-- xsl:apply-templates select="child::Dimension" mode="edges">
    	<xsl:with-param name="source" select="@name"/>
    </xsl:apply-templates -->
  </xsl:template>

  <xsl:template match="Schema/Dimension">
    <xsl:element name="g:node">
      <xsl:attribute name="id">
        <xsl:value-of select="concat('dim.',@name)"/>
      </xsl:attribute>
      <g:data key="d0">
        <y:ShapeNode>
          <y:Geometry height="50.0" width="120.0"/>
          <y:Fill color="#CCFFCC"/>
          <y:NodeLabel>
            <xsl:value-of select="@name"/>
          </y:NodeLabel>
        </y:ShapeNode>
      </g:data>
    </xsl:element>
    <xsl:apply-templates select="child::Hierarchy">
      <xsl:with-param name="source" select="concat('dim.',@name)"/>
    </xsl:apply-templates>
  </xsl:template>

  <xsl:template match="Cube/Dimension">
    <xsl:element name="g:node">
      <xsl:attribute name="id">
        <xsl:value-of select="concat(../@name,'.',@name)"/>
      </xsl:attribute>
      <g:data key="d0">
        <y:ShapeNode>
          <y:Geometry height="50.0" width="120.0"/>
          <y:Fill color="#FFFFCC"/>
          <y:NodeLabel>
            <xsl:value-of select="@name"/>
          </y:NodeLabel>
        </y:ShapeNode>
      </g:data>
    </xsl:element>
    <xsl:apply-templates select="child::Hierarchy">
      <xsl:with-param name="source" select="concat(../@name,'.',@name)"/>
    </xsl:apply-templates>

    <xsl:element name="g:edge">
      <xsl:attribute name="id">
        <xsl:value-of select="generate-id()"/>
      </xsl:attribute>
      <xsl:attribute name="target">
        <xsl:value-of select="concat(../@name,'.',@name)"/>
      </xsl:attribute>
      <xsl:attribute name="source">
        <xsl:value-of select="../@name"/>
      </xsl:attribute>
      <g:data key="d1">
        <y:PolyLineEdge>
          <y:Arrows source="none" target="standard"/>
        </y:PolyLineEdge>
      </g:data>
    </xsl:element>
  </xsl:template>

  <xsl:template match="Cube/DimensionUsage">
    <xsl:element name="g:edge">
      <xsl:attribute name="id">
        <xsl:value-of select="generate-id()"/>
      </xsl:attribute>
      <xsl:attribute name="source">
        <xsl:value-of select="../@name"/>
      </xsl:attribute>
      <xsl:attribute name="target">
        <xsl:value-of select="concat('dim.',@source)"/>
      </xsl:attribute>
      <g:data key="d1">
        <y:PolyLineEdge>
          <y:Arrows source="none" target="standard"/>
        </y:PolyLineEdge>
      </g:data>
    </xsl:element>
  </xsl:template>

  <!-- xsl:template match="Dimension" mode="edges">
  	<xsl:param name="source" select="@source"/>
  	<xsl:param name="target" select="@name"/>
    <xsl:attribute name="target">
      <xsl:value-of select="@name"/>
    </xsl:attribute>
    <xsl:element name="g:edge">
      <xsl:attribute name="id">
        <xsl:value-of select="generate-id()"/>
      </xsl:attribute>
      <xsl:attribute name="target">
        <xsl:value-of select="$target"/>
      </xsl:attribute>
      <xsl:attribute name="source">
        <xsl:value-of select="$source"/>
      </xsl:attribute>
      <g:data key="d1">
        <y:PolyLineEdge>
          <y:Arrows source="none" target="standard"/>
        </y:PolyLineEdge>
      </g:data>
    </xsl:element>
  </xsl:template-->

  <xsl:template match="Hierarchy">
    <xsl:param name="source"/>
    <xsl:variable name="hierid" select="generate-id()"/>
    <!-- xsl:variable name="hierid" select="concat('hier.',../@name,'.',@name)"/ -->
    
    <xsl:element name="g:node">
      <xsl:attribute name="id">
        <xsl:value-of select="$hierid"/>
      </xsl:attribute>
      <g:data key="d0">
        <y:ShapeNode>
          <y:Geometry height="50.0" width="120.0"/>
          <y:Fill color="#CCFFCC"/>
          <y:NodeLabel>
            <xsl:value-of select="@name"/>
			::<xsl:apply-templates select="child::Level" />            
          </y:NodeLabel>
        </y:ShapeNode>
      </g:data>
    </xsl:element>  
    <xsl:element name="g:edge">
      <xsl:attribute name="id">
        <xsl:value-of select="generate-id()"/>
      </xsl:attribute>
      <xsl:attribute name="source">
        <xsl:value-of select="$source"/>
      </xsl:attribute>
      <xsl:attribute name="target">
        <xsl:value-of select="$hierid"/>
      </xsl:attribute>
      <g:data key="d1">
        <y:PolyLineEdge>
          <y:Arrows source="none" target="standard"/>
        </y:PolyLineEdge>
      </g:data>
    </xsl:element>
  </xsl:template>

  <xsl:template match="Level">
l: <xsl:value-of select="@name"/></xsl:template>

  <xsl:template match="Measure">
m: <xsl:value-of select="@name"/></xsl:template>
  
  <xsl:template match="VirtualCube">
    <xsl:element name="g:node">
      <xsl:attribute name="id">
        <xsl:value-of select="@name"/>
      </xsl:attribute>
      <g:data key="d0">
        <y:ShapeNode>
          <y:Geometry height="50.0" width="120.0"/>
          <y:Fill color="#FFCCFF"/>
          <y:NodeLabel>
            <xsl:value-of select="@name"/>
            <xsl:apply-templates select="child::VirtualCubeMeasure" />
            <xsl:apply-templates select="child::CalculatedMember" />
          </y:NodeLabel>
        </y:ShapeNode>
      </g:data>
    </xsl:element>
    <xsl:apply-templates select="child::VirtualCubeDimension" />
  </xsl:template>

  <xsl:template match="VirtualCube/VirtualCubeDimension">
    <xsl:variable name="cube" select="@cubeName"/>
    <xsl:variable name="dim" select="@name"/>
    <!-- xsl:message>
    	virtualcube/virtualdim: <xsl:value-of select="$cube"/>:<xsl:value-of select="$dim"/>
    	:: <xsl:value-of select="count(/Schema/Cube[@name=$cube]/DimensionUsage[@name=$dim]) > 0"/>
    </xsl:message-->
    
    <xsl:element name="g:edge">
      <xsl:attribute name="id">
        <xsl:value-of select="generate-id()"/>
      </xsl:attribute>
      <xsl:attribute name="source">
        <xsl:value-of select="../@name"/>
      </xsl:attribute>
      <xsl:attribute name="target">
      	<xsl:choose>
      	  <xsl:when test="@cubeName!=''">
	      	<xsl:choose>
	      	  <xsl:when test="count(/Schema/Cube[@name=$cube]/DimensionUsage[@name=$dim]) > 0">
            	<!-- xsl:value-of select="concat('dim.',@name)"/ -->
            	<xsl:value-of select="concat('dim.',/Schema/Cube[@name=$cube]/DimensionUsage[@name=$dim]/@source)"/>
              </xsl:when>
              <xsl:otherwise>
            	<xsl:value-of select="concat(@cubeName,'.',@name)"/>
              </xsl:otherwise>
            </xsl:choose>
      	  </xsl:when>
      	  <xsl:otherwise>
            <xsl:value-of select="concat('dim.',@name)"/>
      	  </xsl:otherwise>
      	</xsl:choose>
      </xsl:attribute>
      <g:data key="d1">
        <y:PolyLineEdge>
          <y:Arrows source="none" target="white_delta"/>
        </y:PolyLineEdge>
      </g:data>
    </xsl:element>
  </xsl:template>
  
  <xsl:template match="VirtualCubeMeasure">
m: <xsl:value-of select="@name"/></xsl:template>
  
  <xsl:template match="CalculatedMember">
cm: <xsl:value-of select="@name"/></xsl:template>

  <xsl:template match="Role">
  </xsl:template>
  
</xsl:stylesheet>
