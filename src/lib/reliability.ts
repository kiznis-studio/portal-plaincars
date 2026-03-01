// Vehicle reliability scoring engine
// Computes scores from NHTSA complaint density, recall frequency, and severity data

export interface ReliabilityScore {
  score: number;        // 0-100
  grade: string;        // A+ through F
  complaintRate: number; // complaints per year
  recallRate: number;    // recalls per year
  severityScore: number; // weighted crash+fire+injury+death per year
  yearsCovered: number;
}

export interface ModelReliabilityData {
  model_id: string;
  make_name: string;
  model_name: string;
  slug: string;
  year_count: number;
  first_year: number;
  last_year: number;
  total_complaints: number;
  total_recalls: number;
  total_crashes: number;
  total_fires: number;
  total_injuries: number;
  total_deaths: number;
}

export function calculateReliability(stats: {
  year_count: number;
  total_complaints: number;
  total_recalls: number;
  total_crashes: number;
  total_fires: number;
  total_injuries: number;
  total_deaths: number;
}): ReliabilityScore {
  const years = Math.max(stats.year_count, 1);
  const complaintRate = stats.total_complaints / years;
  const recallRate = stats.total_recalls / years;

  // Severity: deaths × 100 + injuries × 10 + fires × 5 + crashes × 2
  const severityScore = (
    (stats.total_deaths || 0) * 100 +
    (stats.total_injuries || 0) * 10 +
    (stats.total_fires || 0) * 5 +
    (stats.total_crashes || 0) * 2
  ) / years;

  // Normalize each to 0-100 (lower is better → invert)
  const complaintScore = Math.max(0, 100 - (complaintRate / 5) * 100);
  const recallScore = Math.max(0, 100 - (recallRate / 3) * 100);
  const severityScoreNorm = Math.max(0, 100 - (severityScore / 20) * 100);

  // Weighted: complaints 50%, recalls 25%, severity 25%
  const score = Math.round(complaintScore * 0.5 + recallScore * 0.25 + severityScoreNorm * 0.25);

  return {
    score: Math.max(0, Math.min(100, score)),
    grade: scoreToGrade(Math.max(0, Math.min(100, score))),
    complaintRate: Math.round(complaintRate * 10) / 10,
    recallRate: Math.round(recallRate * 10) / 10,
    severityScore: Math.round(severityScore * 10) / 10,
    yearsCovered: years,
  };
}

function scoreToGrade(score: number): string {
  if (score >= 90) return 'A+';
  if (score >= 85) return 'A';
  if (score >= 80) return 'A-';
  if (score >= 75) return 'B+';
  if (score >= 70) return 'B';
  if (score >= 65) return 'B-';
  if (score >= 60) return 'C+';
  if (score >= 55) return 'C';
  if (score >= 50) return 'C-';
  if (score >= 40) return 'D';
  return 'F';
}

export function gradeColor(grade: string): string {
  if (grade.startsWith('A')) return 'text-emerald-500';
  if (grade.startsWith('B')) return 'text-teal-500';
  if (grade.startsWith('C')) return 'text-amber-500';
  if (grade.startsWith('D')) return 'text-orange-500';
  return 'text-amber-600 dark:text-amber-400';
}

export function gradeBg(grade: string): string {
  if (grade.startsWith('A')) return 'bg-emerald-50 dark:bg-emerald-950/30 border-emerald-200 dark:border-emerald-800';
  if (grade.startsWith('B')) return 'bg-teal-50 dark:bg-teal-950/30 border-teal-200 dark:border-teal-800';
  if (grade.startsWith('C')) return 'bg-amber-50 dark:bg-amber-950/30 border-amber-200 dark:border-amber-800';
  if (grade.startsWith('D')) return 'bg-orange-50 dark:bg-orange-950/30 border-orange-200 dark:border-orange-800';
  return 'bg-red-50 dark:bg-red-950/30 border-red-200 dark:border-red-800';
}
