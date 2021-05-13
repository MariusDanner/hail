package is.hail.snpEff;


import is.hail.snpEff.SnpEffectHelper;
import org.snpeff.snpEffect.VariantEffect;
import org.snpeff.snpEffect.VariantEffect.ErrorWarningType;
import org.snpeff.snpEffect.Config;
import org.snpeff.snpEffect.VariantEffects;
import org.snpeff.align.VcfRefAltAlign;
import org.snpeff.interval.Cds;
import org.snpeff.interval.Chromosome;
import org.snpeff.interval.Transcript;
import org.snpeff.interval.Gene;
import org.snpeff.interval.Exon;
import org.snpeff.interval.Genome;
import org.snpeff.interval.Marker;
import org.snpeff.interval.Markers;
import org.snpeff.interval.Variant;
import org.snpeff.interval.Variant.VariantType;
import org.snpeff.interval.VariantBnd;
import org.snpeff.snpEffect.LossOfFunction;
import org.snpeff.snpEffect.EffectType;
import org.snpeff.snpEffect.VariantEffectStructural;
import java.util.List;
import java.sql.Connection;
import java.sql.SQLException;
import java.lang.System;
import org.cache2k.Cache;
import org.cache2k.Cache2kBuilder;
import org.cache2k.CacheManager;
import java.util.concurrent.TimeUnit;

import is.hail.database.DatabaseConnector;

public class SnpEffectDatabasePredictor {

    private static final long serialVersionUID = 4519418862303325081L;

	public static final int DEFAULT_UP_DOWN_LENGTH = 5000;
	public static final int SMALL_VARIANT_SIZE_THRESHOLD = 10;

    private Genome genome;
	private SnpEffectDatabaseHelper snpEffectDbHelper;

    public SnpEffectDatabasePredictor() {
		String configPath = System.getenv().getOrDefault("SNPEFF_CONFIG", "/snpEff.config");
        new Config("GRCh38.99", configPath);
		snpEffectDbHelper = new SnpEffectDatabaseHelper(DatabaseConnector.connectToDatabase(false));

        genome = snpEffectDbHelper.loadGenome("GRCh38.99");

    }

	public static void createCacheIfNotExists() {
		Cache<Integer, Gene> cache = CacheManager.getInstance().getCache("geneCache");
        if (cache == null) {
			new Cache2kBuilder<Integer, Gene>(){}
			.name("geneCache")
			.entryCapacity(100)
			.expireAfterWrite(20, TimeUnit.SECONDS)
			.build();
		}
	}

    public VariantEffects annotateVariant(String chr, Integer position, String ref, String alt) throws SQLException {
        VariantEffects effects = new VariantEffects();
        Chromosome chromo = genome.getChromosome(chr); //TODO load Chromosome somehow
        List<Variant> variants = SnpEffectHelper.variants(chromo, position, ref, alt, null);
        for (Variant variant : variants) {
            VariantEffects effect = variantEffect(variant);
            for (VariantEffect newEffect: effect) {
                effects.add(newEffect);
            }
        }
        return effects;
    }

    public VariantEffects variantEffect(Variant variant) throws SQLException {
        VariantEffects variantEffects = new VariantEffects();

        // if (isChromosomeMissing(variant)) {
		// 	variantEffects.addErrorWarning(variant, ErrorWarningType.ERROR_CHROMOSOME_NOT_FOUND);
		// 	return variantEffects;
		// }

        if (variant.isBnd()) {
			Markers intersects = query(variant); // TODO change this
			variantEffectBnd(variant, variantEffects, intersects);
			return variantEffects;
		}

        // Is this a structural variant? Large structural variants (e.g. involving more than
		// one gene) may require to calculate effects by using all involved genes
		// For some variants we require to be 'N' bases apart (translocations
		// are assumed always involve large genomic regions)
		boolean structuralVariant = variant.isStructural() && (variant.size() > SMALL_VARIANT_SIZE_THRESHOLD);

        Markers intersects = null;

        boolean structuralHuge = structuralVariant && variant.isStructuralHuge();
		if (structuralHuge) {
			// Large variants could make the query results huge and slow down
			// the algorithm, so we stop here
			// Note: Translocations (BND) only intercept two loci, so this
			//       issue does not apply.
			intersects = variantEffectStructuralLarge(variant, variantEffects);
		} else {
			// Query interval tree: Which intervals does variant intersect?
			intersects = query(variant);
		}

        // In case of large structural variants, we need to check the number of genes
		// involved. If more than one, then we need a different approach (e.g. taking
		// into account all genes involved to calculate fusions)");
		if (structuralVariant) {
			// Are we done?
			if (variantEffectStructural(variant, variantEffects, intersects)) return variantEffects;
		}

        variantEffect(variant, variantEffects, intersects);

		return variantEffects;

    }

    protected void variantEffect(Variant variant, VariantEffects variantEffects, Markers intersects) {
		// Show all results
		boolean hitChromo = false, hitSomething = false;
		for (Marker marker : intersects) {
			if (marker instanceof Chromosome) hitChromo = true; // Do we hit any chromosome?
			else {
				// Analyze all markers
				if (variant.isNonRef()) marker.variantEffectNonRef(variant, variantEffects);
				else marker.variantEffect(variant, variantEffects);

				hitSomething = true;
			}
		}

		// Any errors or intergenic (i.e. did not hit any gene)
		if (!hitChromo) {
			// Special case: Insertion right after chromosome's last base
			Chromosome chr = genome.getChromosome(variant.getChromosomeName());
			if (variant.isIns() && variant.getStart() == (chr.getEnd() + 1)) {
				// This is a chromosome extension
				variantEffects.add(variant, null, EffectType.CHROMOSOME_ELONGATION, "");
			} else if (Config.get().isErrorChromoHit()) {
				variantEffects.addErrorWarning(variant, ErrorWarningType.ERROR_OUT_OF_CHROMOSOME_RANGE);
			}
		} else if (!hitSomething) {
			if (Config.get().isOnlyRegulation()) {
				variantEffects.add(variant, null, EffectType.NONE, "");
			} else {
				variantEffects.add(variant, null, EffectType.INTERGENIC, "");
			}
		}
	}

    public void variantEffectBnd(Variant variant, VariantEffects variantEffects, Markers intersects) {
		// Create a new variant effect for structural variants, then calculate all transcript fusions
		VariantEffectStructural veff = new VariantEffectStructural(variant, intersects);

		// Do we have a fusion event?
		List<VariantEffect> veffFusions = veff.fusions();
		if (veffFusions != null) {
			for (VariantEffect veffFusion : veffFusions)
				variantEffects.add(veffFusion);
		}
	}

    public boolean variantEffectStructural(Variant variant, VariantEffects variantEffects, Markers intersects) {
		// Any variant effects added?
		boolean added = false;

		// Create a new variant effect for structural variants, add effect (if any)
		VariantEffectStructural veff = new VariantEffectStructural(variant, intersects);
		if (veff.getEffectType() != EffectType.NONE) {
			variantEffects.add(veff);
			added = true;
		}

		// Do we have a fusion event?
		List<VariantEffect> veffFusions = veff.fusions();
		if (veffFusions != null && !veffFusions.isEmpty()) {
			for (VariantEffect veffFusion : veffFusions) {
				added = true;
				variantEffects.add(veffFusion);
			}
		}

		// In some cases we want to annotate all overlapping genes
		if (variant.isDup() || variant.isDel()) return false;

		// If variant effects were added, there is no need for further analysis
		return added;
	}

    public Markers variantEffectStructuralLarge(Variant variant, VariantEffects variantEffects) {
		EffectType eff, effGene, effTr, effExon, effExonPartial;

		switch (variant.getVariantType()) {
		case DEL:
			eff = EffectType.CHROMOSOME_LARGE_DELETION;
			effGene = EffectType.GENE_DELETED;
			effTr = EffectType.TRANSCRIPT_DELETED;
			effExon = EffectType.EXON_DELETED;
			effExonPartial = EffectType.EXON_DELETED_PARTIAL;
			break;

		case DUP:
			eff = EffectType.CHROMOSOME_LARGE_DUPLICATION;
			effGene = EffectType.GENE_DUPLICATION;
			effTr = EffectType.TRANSCRIPT_DUPLICATION;
			effExon = EffectType.EXON_DUPLICATION;
			effExonPartial = EffectType.EXON_DUPLICATION_PARTIAL;
			break;

		case INV:
			eff = EffectType.CHROMOSOME_LARGE_INVERSION;
			effGene = EffectType.GENE_INVERSION;
			effTr = EffectType.TRANSCRIPT_INVERSION;
			effExon = EffectType.EXON_INVERSION;
			effExonPartial = EffectType.EXON_INVERSION_PARTIAL;
			break;

		default:
			throw new RuntimeException("Unimplemented option for variant type " + variant.getVariantType());
		}

		// Add effect
		variantEffects.add(variant, variant.getChromosome(), eff, "");

		// Add detailed effects for genes & transcripts
		return variantEffectStructuralLargeGenes(variant, variantEffects, effGene, effTr, effExon, effExonPartial);
	}

	/**
	 * Add large structural variant effects: Genes and transcripts
	 */
	public Markers variantEffectStructuralLargeGenes(Variant variant, VariantEffects variantEffects, EffectType effGene, EffectType effTr, EffectType effExon, EffectType effExonPartial) {
		Markers intersect = new Markers();

		// Check all genes in the genome
		for (Gene g : genome.getGenes()) {
			// Does the variant affect the gene?
			if (variant.intersects(g)) {
				intersect.add(g);
				variantEffects.add(variant, g, effGene, "");

				// Does the variant affect this transcript?
				for (Transcript tr : g) {
					// Variant affects the whole transcript?
					if (variant.includes(tr)) {
						intersect.add(tr);
						variantEffects.add(variant, tr, effTr, "");
					} else if (variant.intersects(tr)) {
						intersect.add(tr);

						// Variant affects part of the transcript
						// Add effects for each exon
						for (Exon ex : tr) {
							if (variant.includes(ex)) {
								variantEffects.add(variant, ex, effExon, "");
							} else if (variant.intersects(ex)) {
								variantEffects.add(variant, ex, effExonPartial, "");
							}
						}
					}
				}
			}
		}

		return intersect;
	}

    boolean isChromosomeMissing(Marker marker) {
		// Missing chromosome in marker?
		if (marker.getChromosome() == null) return true;

		// Missing chromosome in genome?
		String chrName = marker.getChromosomeName();
		Chromosome chr = genome.getChromosome(chrName);
		if (chr == null) return true;

		// Chromosome length is 1 or less?
		if (chr.size() < 1) return true;

		// OK, we have the chromosome
		return false;
	}

    private Markers query(Variant variant) throws SQLException {
        List<Marker> candidates = snpEffectDbHelper.loadGenes(variant);
		Markers markers = new Markers();
		for (Marker c: candidates) {
			if (c.intersects(variant)) {
				markers.add(c);
			}
		}
        markers.add(variant.getChromosome());
        return markers;
    }


}