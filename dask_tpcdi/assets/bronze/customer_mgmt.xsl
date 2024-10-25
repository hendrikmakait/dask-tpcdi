<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xmlns:TPCDI="https://www.tpc.org/tpcdi">
    <xsl:output method="xml" omit-xml-declaration="no" indent="yes" />
    <xsl:strip-space elements="*" />
    <xsl:template match="/TPCDI:Actions">
        <xsl:copy>
            <xsl:apply-templates select="Customer" />
        </xsl:copy>
    </xsl:template>
    <xsl:template match="TPCDI:Action" name="Customer">
        <xsl:copy>
            
        </xsl:copy>
    </xsl:template>
</xsl:stylesheet>