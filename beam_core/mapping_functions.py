def map_ccdsGene_to_dict(row):
    """Mapeia uma linha da tabela 'ccdsGene' para um dicionário."""
    return {
        "bin": row.bin, "cdsEnd": row.cdsEnd, "cdsEndStat": row.cdsEndStat,
        "cdsStart": row.cdsStart, "cdsStartStat": row.cdsStartStat,
        "chrom": row.chrom, "exonCount": row.exonCount, "exonEnds": row.exonEnds,
        "exonFrames": row.exonFrames, "exonStarts": row.exonStarts,
        "name": row.name, "name2": row.name2, "score": row.score,
        "strand": row.strand, "txEnd": row.txEnd, "txStart": row.txStart,
    }

def map_ccdsInfo_to_dict(row):
    """Mapeia uma linha da tabela 'ccdsInfo' para um dicionário."""
    return {
        "ccds": row.ccds, "mrnaAcc": row.mrnaAcc,
        "protAcc": row.protAcc, "srcDb": row.srcDb,
    }

# Dicionário central que exporta todas as funções de mapeamento disponíveis
MAP_FUNCTIONS = {
    "map_ccdsGene_to_dict": map_ccdsGene_to_dict,
    "map_ccdsInfo_to_dict": map_ccdsInfo_to_dict,
}