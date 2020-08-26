import argparse
import hail as hl


def run_gwas(vcf_file, phenotypes_file, output_file):
    table = hl.import_table(phenotypes_file, impute=True).key_by('Sample')

    hl.import_vcf(vcf_file).write('tmp.mt')
    mt = hl.read_matrix_table('tmp.mt')

    mt = mt.annotate_cols(pheno=table[mt.s])

    downsampled = mt.sample_rows(0.01, seed=11223344)
    eigenvalues, pcs, _ = hl.hwe_normalized_pca(downsampled.GT)

    mt = mt.annotate_cols(scores=pcs[mt.s].scores)

    gwas = hl.linear_regression_rows(
        y=mt.pheno.CaffeineConsumption,
        x=mt.GT.n_alt_alleles(),
        covariates=[1.0, mt.scores[0], mt.scores[1], mt.scores[2]])

    gwas = gwas.select(SNP=hl.variant_str(gwas.locus, gwas.alleles), P=gwas.p_value)
    gwas = gwas.key_by(gwas.SNP)
    gwas = gwas.select(gwas.P)
    gwas.export(f'{output_file}.assoc', header=True)

    hl.export_plink(mt, output_file, fam_id=mt.s, ind_id=mt.s)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--vcf', required=True)
    parser.add_argument('--phenotypes', required=True)
    parser.add_argument('--output-file', required=True)
    parser.add_argument('--cores', required=False)
    args = parser.parse_args()

    if args.cores:
        hl.init(master=f'local[{args.cores}]')

    run_gwas(args.vcf, args.phenotypes, args.output_file)
