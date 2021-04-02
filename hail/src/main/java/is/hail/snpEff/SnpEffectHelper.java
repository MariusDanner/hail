package is.hail.snpEff;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.snpeff.align.VcfRefAltAlign;
import org.snpeff.interval.Cds;
import org.snpeff.interval.Chromosome;
import org.snpeff.interval.Marker;
import org.snpeff.interval.Variant;
import org.snpeff.interval.Variant.VariantType;
import org.snpeff.interval.VariantBnd;
import org.snpeff.snpEffect.LossOfFunction;


public class SnpEffectHelper {

    public static List<Variant> variants(Chromosome chromo, int start, String reference, String alt, String id) {
		List<Variant> list = null;
        int end = start + reference.length() - 1;
		if (alt != null) alt = alt.toUpperCase();

		if (alt == null || alt.isEmpty() || alt.equals(reference)) {
			// Non-variant
			list = Variant.factory(chromo, start, reference, null, id, false);
		} else if (alt.charAt(0) == '<') {
			// Structural variants
			if (alt.startsWith("<DEL")) {
				// Case: Deletion
				// 2 321682    .  T   <DEL>         6     PASS    IMPRECISE;SVTYPE=DEL;END=321887;SVLEN=-105;CIPOS=-56,20;CIEND=-10,62
				String ch = reference;
				int startNew = start;

				if (end > start) {
					startNew = start + reference.length();
					int size = end - startNew + 1;
					char change[] = new char[size];
					for (int i = 0; i < change.length; i++)
						change[i] = reference.length() > i ? reference.charAt(i) : 'N';
					ch = new String(change);
				}
				list = Variant.factory(chromo, startNew, ch, "", id, false);
			} else if (alt.startsWith("<INV")) {
				// Inversion
				int startNew = start + reference.length();
				Variant var = new Variant(chromo, startNew, end, id);
				var.setVariantType(VariantType.INV);
				list = new LinkedList<>();
				list.add(var);
			} else if (alt.startsWith("<DUP")) {
				// Duplication
				int startNew = start + reference.length();
				Variant var = new Variant(chromo, startNew, end, id);
				var.setVariantType(VariantType.DUP);
				list = new LinkedList<>();
				list.add(var);
			}
		} else if ((alt.indexOf('[') >= 0) || (alt.indexOf(']') >= 0)) {
			// Translocations

			// Parse ALT string
			boolean left = alt.indexOf(']') >= 0;
			String sep = (left ? "\\]" : "\\[");
			String tpos[] = alt.split(sep);
			String pos = tpos[1];
			boolean before = (alt.indexOf(']') == 0) || (alt.indexOf('[') == 0);
			String altBases = (before ? tpos[2] : tpos[0]);

			// Parse 'chr:start'
			String posSplit[] = pos.split(":");
			String trChrName = posSplit[0];
			Chromosome trChr = chromo.getGenome().getOrCreateChromosome(trChrName);
			int trStart = parseIntSafe(posSplit[1]) - 1;

			VariantBnd var = new VariantBnd(chromo, start, reference, altBases, trChr, trStart, left, before);
			list = new LinkedList<>();
			list.add(var);
		} else if (reference.length() == alt.length()) {
			// Case: SNP, MNP
			if (reference.length() == 1) {
				// SNPs
				// 20     3 .         C      G       .   PASS  DP=100
				list = Variant.factory(chromo, start, reference, alt, id, true);
			} else {
				// MNPs
				// 20     3 .         TC     AT      .   PASS  DP=100
				// Sometimes the first bases are the same and we can trim them
				int startDiff = Integer.MAX_VALUE;
				for (int i = 0; i < reference.length(); i++)
					if (reference.charAt(i) != alt.charAt(i)) startDiff = Math.min(startDiff, i);

				// MNPs
				// Sometimes the last bases are the same and we can trim them
				int endDiff = 0;
				for (int i = reference.length() - 1; i >= 0; i--)
					if (reference.charAt(i) != alt.charAt(i)) endDiff = Math.max(endDiff, i);

				String newRef = reference.substring(startDiff, endDiff + 1);
				String newAlt = alt.substring(startDiff, endDiff + 1);
				list = Variant.factory(chromo, start + startDiff, newRef, newAlt, id, true);
			}
		} else {
			// Short Insertions, Deletions or Mixed Variants (substitutions)
			VcfRefAltAlign align = new VcfRefAltAlign(alt, reference);
			align.align();
			int startDiff = align.getOffset();

			switch (align.getVariantType()) {
			case DEL:
				// Case: Deletion
				// 20     2 .         TC      T      .   PASS  DP=100
				// 20     2 .         AGAC    AAC    .   PASS  DP=100
				String ref = "";
				String ch = align.getAlignment();
				if (!ch.startsWith("-")) throw new RuntimeException("Deletion '" + ch + "' does not start with '-'. This should never happen!");
				list = Variant.factory(chromo, start + startDiff, ref, ch, id, true);
				break;

			case INS:
				// Case: Insertion of A { tC ; tCA } tC is the reference allele
				// 20     2 .         TC      TCA    .   PASS  DP=100
				ch = align.getAlignment();
				ref = "";
				if (!ch.startsWith("+")) throw new RuntimeException("Insertion '" + ch + "' does not start with '+'. This should never happen!");
				list = Variant.factory(chromo, start + startDiff, ref, ch, id, true);
				break;

			case MIXED:
				// Case: Mixed variant (substitution)
				reference = reference.substring(startDiff);
				alt = alt.substring(startDiff);
				list = Variant.factory(chromo, start + startDiff, reference, alt, id, true);
				break;

			default:
				// Other change type?
				throw new RuntimeException("Unsupported VCF change type '" + align.getVariantType() + "'\n\tRef: " + reference + "'\n\tAlt: '" + alt + "'\n\tVcfEntry: " + start);
			}
		}

		//---
		// Add original 'ALT' field as genotype
		//---
		if (list == null) list = new LinkedList<>();
		for (Variant variant : list)
			variant.setGenotype(alt);

		return list;
	}

	public static int parseIntSafe(String s) {
		try {
			return Integer.parseInt(s);
		} catch (Exception e) {
			return 0;
		}
	}
}