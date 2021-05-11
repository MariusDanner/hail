package is.hail.snpEff;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.cache2k.Cache;
import org.cache2k.CacheManager;

import org.snpeff.interval.Gene;
import org.snpeff.interval.Cds;
import org.snpeff.interval.Chromosome;
import org.snpeff.interval.BioType;
import org.snpeff.interval.Genome;
import org.snpeff.interval.Intergenic;
import org.snpeff.interval.Exon;
import org.snpeff.interval.Exon.ExonSpliceType;
import org.snpeff.interval.Marker;
import org.snpeff.interval.Transcript;
import org.snpeff.interval.TranscriptSupportLevel;
import org.snpeff.interval.Variant;
import org.snpeff.interval.Variant.VariantType;
import org.snpeff.interval.VariantBnd;
import org.snpeff.interval.Utr;
import org.snpeff.interval.Utr3prime;
import org.snpeff.interval.Utr5prime;
import org.snpeff.snpEffect.LossOfFunction;


public class SnpEffectDatabaseHelper {

    public static final int DEFAULT_UP_DOWN_LENGTH = 5000;

    private Cache<Integer, Gene> geneCache;
    private Connection connection;

    public SnpEffectDatabaseHelper(Connection conn) {
        connection = conn;
        geneCache = CacheManager.getInstance().getCache("geneCache");
    }

    public Genome loadGenome(String id) {
        String query = "SELECT species, version, chromosomes FROM snpEff_genomes WHERE id = ?";
        try {
            PreparedStatement stmt = connection.prepareStatement(query);
            stmt.setString(1, id);
            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                Genome genome = new Genome(rs.getString("version"));
                // genome.species = rs.getString("species")
                String[] chromosomeIds = rs.getString("chromosomes").split(",");
                List<Chromosome> chromosomes = loadChromosomes(Arrays.asList(chromosomeIds), genome);
                for (Chromosome chr : chromosomes) {
                    genome.add(chr);
                }
                return genome;
            }
        } catch (Exception e) {
            System.out.println(e);
        }
        return new Genome();
	}

	public List<Chromosome> loadChromosomes(List<String> chromosomes, Genome parentGenome) throws SQLException {
        StringBuilder placeholderBuilder = new StringBuilder();
        chromosomes.forEach(chromosome -> {
        placeholderBuilder.append("'");
        placeholderBuilder.append(chromosome);
        placeholderBuilder.append("',");
        });
        String placeHolders =  placeholderBuilder.deleteCharAt( placeholderBuilder.length() -1 ).toString();

        String query = "SELECT intervalstart, intervalend, id FROM snpEff_chromosomes WHERE internalid in (" + placeHolders + ")";
        PreparedStatement stmt = connection.prepareStatement(query);
        ResultSet rs = stmt.executeQuery();

        List<Chromosome> chrs = new ArrayList<>();
        while (rs.next()) {
            chrs.add(new Chromosome(parentGenome, rs.getInt("intervalstart"), rs.getInt("intervalend"), rs.getString("id")));
        }
        return chrs;
    }

    public List<Marker> loadGenes(Variant variant) throws SQLException {
        Chromosome chr = variant.getChromosome();

        String query = "SELECT g.internalid, g.id, g.intervalstart, g.intervalend, g.strandminus, g.name, g.biotype from snpeff_genes g JOIN snpeff_chromosomes c ON c.internalid = g.parent WHERE c.id = ? AND LEAST(g.intervalend + ?, ?) >= GREATEST(g.intervalstart - ?, ?)";
        PreparedStatement stmt = connection.prepareStatement(query);
        stmt.setString(1, chr.getId()); //TODO
        stmt.setInt(2, DEFAULT_UP_DOWN_LENGTH);
        stmt.setInt(3, variant.getStart());
        stmt.setInt(4, DEFAULT_UP_DOWN_LENGTH);
        stmt.setInt(5, variant.getEnd());
        ResultSet rs = stmt.executeQuery();

        List<Marker> genes = new ArrayList<Marker>();
        while (rs.next()) {
            Gene gene = createGeneFromRs(rs, chr);
            if (gene != null) {
                genes.add(gene);
                for (Transcript t : gene) {
                    genes.add(t.getUpstream());
                    genes.add(t.getDownstream());
                }
            }
        }
        if (genes.isEmpty()) {
            // add Intergenic regions
            Intergenic intergenic = loadIntergenic(variant);
            // if (intergenic != null) {

            // }
            genes.add(intergenic);
        }
        return genes;
    }

    private Gene createGeneFromRs(ResultSet rs, Chromosome chr) throws SQLException {
        Gene gene = null;
        Integer internalId = rs.getInt("internalId");
        if (geneCache.containsKey(internalId)) {
            gene = geneCache.peek(internalId);
        } else {
            gene = new Gene(chr, rs.getInt("intervalstart"), rs.getInt("intervalend"), rs.getBoolean("strandminus"), rs.getString("id"), rs.getString("name"), BioType.parse(rs.getString("biotype")));
            List<Transcript> transcripts = loadTranscripts(internalId, gene);
            for (Transcript t : transcripts) {
                gene.add(t);
            }
            geneCache.put(internalId, gene);
        }
        return gene;
    }

    public Intergenic loadIntergenic(Variant variant) throws SQLException {
        Chromosome chr = variant.getChromosome();
        Gene leftGene = null;
        Gene rightGene = null;
        String leftQuery = "SELECT g.internalid, g.id, g.intervalstart, g.intervalend, g.strandminus, g.name, g.biotype, ? - g.intervalend as dist from snpeff_genes g JOIN snpeff_chromosomes c on c.internalid = g.parent where c.id = ? and g.intervalend < ? order by dist asc limit 1";
        PreparedStatement leftStmt = connection.prepareStatement(leftQuery);
        leftStmt.setInt(1, variant.getStart());
        leftStmt.setString(2, chr.getId());
        leftStmt.setInt(3, variant.getStart());
        ResultSet leftRs = leftStmt.executeQuery();

        if (leftRs.next()) {
            leftGene = createGeneFromRs(leftRs, chr);
        }

        String rightQuery = "SELECT g.internalid, g.id, g.intervalstart, g.intervalend, g.strandminus, g.name, g.biotype, g.intervalstart - ? as dist from snpeff_genes g JOIN snpeff_chromosomes c on c.internalid = g.parent where c.id = ? and g.intervalstart > ? order by dist asc limit 1";
        PreparedStatement rightStmt = connection.prepareStatement(rightQuery);
        rightStmt.setInt(1, variant.getEnd());
        rightStmt.setString(2, chr.getId());
        rightStmt.setInt(3, variant.getEnd());
        ResultSet rightRs = rightStmt.executeQuery();
        if (rightRs.next()) {
            rightGene = createGeneFromRs(rightRs, chr);
            }

        return Intergenic.createIntergenic(leftGene, rightGene);


    }

    public List<Transcript> loadTranscripts(Integer geneId, Gene gene) throws SQLException {

        String query = "SELECT internalid, id, intervalstart, intervalend, strandminus, subintervals, biotype, proteincoding, dnacheck, aacheck, corrected, ribosomalslippage, transcriptsupportlevel, version, utrs, cdss from snpeff_transcripts WHERE parent = ?";
        PreparedStatement stmt = connection.prepareStatement(query);
        stmt.setInt(1, geneId); //TODO
        ResultSet rs = stmt.executeQuery();

        List<Transcript> transcripts = new ArrayList<>();
        while (rs.next()) {
            Integer internalId = rs.getInt("internalId");
            Transcript transcript = new Transcript(gene, rs.getInt("intervalstart"), rs.getInt("intervalend"), rs.getBoolean("strandminus"), rs.getString("id"));
            transcript.setAaCheck(rs.getBoolean("aacheck"));
            transcript.setDnaCheck(rs.getBoolean("dnacheck"));
            transcript.setProteinCoding(rs.getBoolean("proteincoding"));
            transcript.setRibosomalSlippage(rs.getBoolean("ribosomalslippage"));
            transcript.setBioType(BioType.parse(rs.getString("biotype")));
            transcript.setTranscriptSupportLevel(TranscriptSupportLevel.parse(rs.getString("transcriptsupportlevel")));
            transcript.setVersion(rs.getString("version"));

            List<Exon> exons = loadExons(internalId, transcript);
            for (Exon e : exons) {
                transcript.add(e);
            }
            // List<Cds> cdss = loadCdss(internalId, transcript);
            // for (Cds c : cdss) {
            //     transcript.add(c);
            // }
            transcript.createUpDownStream(DEFAULT_UP_DOWN_LENGTH);
            transcripts.add(transcript);
        }
        return transcripts;
    }

    public List<Exon> loadExons(Integer transcriptId, Transcript transcript) throws SQLException {

        String query = "SELECT internalid, id, intervalstart, intervalend, strandminus, sequence, frame, rank, exontype from snpeff_exons WHERE parent = ?";
        PreparedStatement stmt = connection.prepareStatement(query);
        stmt.setInt(1, transcriptId); //TODO
        ResultSet rs = stmt.executeQuery();

        List<Exon> exons = new ArrayList<>();
        while (rs.next()) {
            Integer internalId = rs.getInt("internalid");
            Exon exon = new Exon(transcript, rs.getInt("intervalstart"), rs.getInt("intervalend"), rs.getBoolean("strandminus"), rs.getString("id"), rs.getInt("rank"));
            exon.setFrame(rs.getInt("frame"));
            exon.setSequence(rs.getString("sequence"));
            exon.setSpliceType(ExonSpliceType.valueOf(rs.getString("exontype")));

            // List<Utr> utrs = loadUtrs(internalId, exon);
            // for (Utr u : utrs) {
            //     transcript.add(u);
            // }
            exons.add(exon);
        }
        return exons;
    }



    public List<Cds> loadCdss(Integer transcriptId, Transcript transcript) throws SQLException {

        String query = "SELECT id, intervalstart, intervalend, strandminus, frame from snpeff_cdss WHERE parent = ?";
        PreparedStatement stmt = connection.prepareStatement(query);
        stmt.setInt(1, transcriptId); //TODO
        ResultSet rs = stmt.executeQuery();

        List<Cds> cdss = new ArrayList<>();
        while (rs.next()) {
            Cds cds = new Cds(transcript, rs.getInt("intervalstart"), rs.getInt("intervalend"), rs.getBoolean("strandminus"), rs.getString("id"));
            cds.setFrame(rs.getInt("frame"));
            cdss.add(cds);
        }
        return cdss;
    }

    public List<Utr> loadUtrs(Integer exonId, Exon exon) throws SQLException {

        String query = "SELECT type, id, intervalstart, intervalend, strandminus from snpeff_utrs WHERE parent = ?";
        PreparedStatement stmt = connection.prepareStatement(query);
        stmt.setInt(1, exonId); //TODO
        ResultSet rs = stmt.executeQuery();

        List<Utr> utrs = new ArrayList<>();
        while (rs.next()) {
            Utr utr = null;
            if ("UTR_5_PRIME".equals(rs.getString("type"))) {
                utr = new Utr5prime(exon, rs.getInt("intervalstart"), rs.getInt("intervalend"), rs.getBoolean("strandminus"), rs.getString("id"));
            } else {
                utr = new Utr3prime(exon, rs.getInt("intervalstart"), rs.getInt("intervalend"), rs.getBoolean("strandminus"), rs.getString("id"));
            }
            utrs.add(utr);
        }
        return utrs;
    }


}