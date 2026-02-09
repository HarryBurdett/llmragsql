import { useState, useEffect } from 'react';
import { useQuery, useMutation } from '@tanstack/react-query';
import {
  Download,
  FileText,
  Users,
  CheckCircle,
  XCircle,
  AlertTriangle,
  Loader2,
  ArrowRight,
  ArrowLeft,
  Building2,
  CreditCard
} from 'lucide-react';

const API_BASE = 'http://localhost:8000/api';

// Types
interface EmployeeGroup {
  code: string;
  description: string;
  employee_count: number;
}

interface PensionScheme {
  code: string;
  description: string;
  provider_name: string;
  provider_reference: string;
  scheme_reference: string;
  employer_rate: number;
  employee_rate: number;
  auto_enrolment: boolean;
  scheme_type: number;
  enrolled_count: number;
}

interface PayrollPeriod {
  tax_year: string;
  period: number;
  pay_date: string | null;
  employee_count: number;
}

interface ContributionEmployee {
  employee_ref: string;
  surname: string;
  forename: string;
  ni_number: string;
  group: string;
  pensionable_earnings: number;
  employee_contribution: number;
  employer_contribution: number;
  is_new_starter: boolean;
  is_leaver: boolean;
}

interface ContributionSummary {
  total_employees: number;
  new_starters: number;
  leavers: number;
  total_pensionable_earnings: number;
  total_employee_contributions: number;
  total_employer_contributions: number;
}

type WizardStep = 'groups' | 'scheme' | 'employees' | 'summary';

export function PensionExport() {
  // Wizard state
  const [currentStep, setCurrentStep] = useState<WizardStep>('groups');

  // Selection state
  const [selectedGroups, setSelectedGroups] = useState<string[]>([]);
  const [selectedScheme, setSelectedScheme] = useState<string>('');
  const [selectedTaxYear, setSelectedTaxYear] = useState<string>('');
  const [selectedPeriod, setSelectedPeriod] = useState<number>(0);
  const [paymentSource, setPaymentSource] = useState<string>('Bank Account');
  const [selectedEmployees, setSelectedEmployees] = useState<string[]>([]);
  const [autoAddEmployees, setAutoAddEmployees] = useState(true);

  // Process options
  const [processType, setProcessType] = useState<'report' | 'file'>('file');

  // Export state
  const [exporting, setExporting] = useState(false);
  const [exportResult, setExportResult] = useState<{
    success: boolean;
    filename?: string;
    content?: string;
    errors?: string[];
    warnings?: string[];
  } | null>(null);

  // Fetch employee groups
  const { data: groupsData, isLoading: groupsLoading } = useQuery({
    queryKey: ['employeeGroups'],
    queryFn: async () => {
      const res = await fetch(`${API_BASE}/pension/employee-groups`);
      return res.json();
    },
  });

  // Fetch pension schemes
  const { data: schemesData, isLoading: schemesLoading } = useQuery({
    queryKey: ['pensionSchemes'],
    queryFn: async () => {
      const res = await fetch(`${API_BASE}/pension/schemes`);
      return res.json();
    },
  });

  // Fetch payroll periods
  const { data: periodsData } = useQuery({
    queryKey: ['payrollPeriods', selectedTaxYear],
    queryFn: async () => {
      const url = selectedTaxYear
        ? `${API_BASE}/pension/payroll-periods?tax_year=${selectedTaxYear}`
        : `${API_BASE}/pension/payroll-periods`;
      const res = await fetch(url);
      return res.json();
    },
  });

  // Fetch contributions based on selections
  const { data: contributionsData, isLoading: contributionsLoading } = useQuery({
    queryKey: ['contributions', selectedScheme, selectedTaxYear, selectedPeriod, selectedGroups],
    queryFn: async () => {
      if (!selectedScheme || !selectedTaxYear || !selectedPeriod) return null;

      const groupsParam = selectedGroups.length > 0 ? `&group_codes=${selectedGroups.join(',')}` : '';
      const res = await fetch(
        `${API_BASE}/pension/contributions?scheme_code=${selectedScheme}&tax_year=${selectedTaxYear}&period=${selectedPeriod}${groupsParam}`
      );
      return res.json();
    },
    enabled: !!selectedScheme && !!selectedTaxYear && !!selectedPeriod,
  });

  // Set defaults when data loads
  useEffect(() => {
    if (periodsData?.tax_year && !selectedTaxYear) {
      setSelectedTaxYear(periodsData.tax_year);
    }
    if (periodsData?.periods?.length > 0 && !selectedPeriod) {
      setSelectedPeriod(periodsData.periods[0].period);
    }
  }, [periodsData]);

  // Auto-populate employees when contributions load
  useEffect(() => {
    if (autoAddEmployees && contributionsData?.contributions) {
      setSelectedEmployees(contributionsData.contributions.map((c: ContributionEmployee) => c.employee_ref));
    }
  }, [contributionsData, autoAddEmployees]);

  // Get current scheme details
  const currentScheme = schemesData?.schemes?.find((s: PensionScheme) => s.code === selectedScheme);

  // Get provider for current scheme
  const getProviderKey = (schemeType: number): string => {
    const typeToProvider: Record<number, string> = {
      1: 'aviva',
      2: 'scottish_widows',
      3: 'smart_pension',
      4: 'peoples_pension',
      5: 'royal_london',
      6: 'standard_life',
      7: 'legal_general',
      8: 'aegon',
      11: 'nest'
    };
    return typeToProvider[schemeType] || 'nest';
  };

  // Export mutation
  const exportMutation = useMutation({
    mutationFn: async () => {
      if (!currentScheme) throw new Error('No scheme selected');

      const providerKey = getProviderKey(currentScheme.scheme_type);
      const employeeRefs = selectedEmployees.length > 0 ? `&employee_refs=${selectedEmployees.join(',')}` : '';
      const groupsParam = selectedGroups.length > 0 ? `&group_codes=${selectedGroups.join(',')}` : '';

      const res = await fetch(
        `${API_BASE}/pension/generate?provider=${providerKey}&scheme_code=${selectedScheme}&tax_year=${selectedTaxYear}&period=${selectedPeriod}&payment_source=${encodeURIComponent(paymentSource)}${groupsParam}${employeeRefs}`,
        { method: 'POST' }
      );
      return res.json();
    },
    onSuccess: (data) => {
      setExportResult(data);
      if (data.success && (data.csv_content || data.content)) {
        // Trigger download
        const content = data.csv_content || data.content;
        const blob = new Blob([content], { type: 'text/csv' });
        const url = window.URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = data.filename;
        document.body.appendChild(a);
        a.click();
        window.URL.revokeObjectURL(url);
        document.body.removeChild(a);
      }
    },
    onError: (error) => {
      setExportResult({
        success: false,
        errors: [error instanceof Error ? error.message : 'Export failed']
      });
    }
  });

  const handleExport = () => {
    setExporting(true);
    exportMutation.mutate();
    setExporting(false);
  };

  // Step navigation
  const steps: WizardStep[] = ['groups', 'scheme', 'employees', 'summary'];
  const currentStepIndex = steps.indexOf(currentStep);

  const canProceed = () => {
    switch (currentStep) {
      case 'groups':
        return true; // Groups are optional
      case 'scheme':
        return !!selectedScheme && !!selectedTaxYear && !!selectedPeriod;
      case 'employees':
        return selectedEmployees.length > 0;
      case 'summary':
        return true;
      default:
        return false;
    }
  };

  const goNext = () => {
    if (currentStepIndex < steps.length - 1) {
      setCurrentStep(steps[currentStepIndex + 1]);
    }
  };

  const goBack = () => {
    if (currentStepIndex > 0) {
      setCurrentStep(steps[currentStepIndex - 1]);
    }
  };

  // Calculate summary from selected employees
  const getSummary = (): ContributionSummary => {
    if (!contributionsData?.contributions) {
      return {
        total_employees: 0,
        new_starters: 0,
        leavers: 0,
        total_pensionable_earnings: 0,
        total_employee_contributions: 0,
        total_employer_contributions: 0
      };
    }

    const selected = contributionsData.contributions.filter(
      (c: ContributionEmployee) => selectedEmployees.includes(c.employee_ref)
    );

    return {
      total_employees: selected.length,
      new_starters: selected.filter((c: ContributionEmployee) => c.is_new_starter).length,
      leavers: selected.filter((c: ContributionEmployee) => c.is_leaver).length,
      total_pensionable_earnings: selected.reduce((sum: number, c: ContributionEmployee) => sum + c.pensionable_earnings, 0),
      total_employee_contributions: selected.reduce((sum: number, c: ContributionEmployee) => sum + c.employee_contribution, 0),
      total_employer_contributions: selected.reduce((sum: number, c: ContributionEmployee) => sum + c.employer_contribution, 0)
    };
  };

  const formatCurrency = (amount: number) => {
    return new Intl.NumberFormat('en-GB', { style: 'currency', currency: 'GBP' }).format(amount);
  };

  const formatPeriod = (taxYear: string, period: number) => {
    const yearStart = 2000 + parseInt(taxYear.slice(0, 2));
    const months = ['Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec', 'Jan', 'Feb', 'Mar'];
    return `${months[period - 1]} ${period >= 10 ? yearStart + 1 : yearStart}`;
  };

  return (
    <div className="p-6 max-w-6xl mx-auto">
      <div className="mb-6">
        <h1 className="text-2xl font-bold text-gray-900 flex items-center gap-2">
          <FileText className="w-6 h-6" />
          Pension Contribution Export
        </h1>
        <p className="text-gray-600 mt-1">
          Generate contribution schedule files for pension providers
        </p>
      </div>

      {/* Progress Steps */}
      <div className="mb-8">
        <div className="flex items-center justify-between">
          {steps.map((step, index) => (
            <div key={step} className="flex items-center">
              <div
                className={`flex items-center justify-center w-10 h-10 rounded-full border-2 ${
                  index <= currentStepIndex
                    ? 'bg-blue-600 border-blue-600 text-white'
                    : 'border-gray-300 text-gray-500'
                }`}
              >
                {index < currentStepIndex ? (
                  <CheckCircle className="w-5 h-5" />
                ) : (
                  <span>{index + 1}</span>
                )}
              </div>
              <span className={`ml-2 text-sm font-medium ${
                index <= currentStepIndex ? 'text-blue-600' : 'text-gray-500'
              }`}>
                {step === 'groups' && 'Select Groups'}
                {step === 'scheme' && 'Scheme & Period'}
                {step === 'employees' && 'Employees'}
                {step === 'summary' && 'Summary'}
              </span>
              {index < steps.length - 1 && (
                <div className={`w-16 h-0.5 mx-4 ${
                  index < currentStepIndex ? 'bg-blue-600' : 'bg-gray-300'
                }`} />
              )}
            </div>
          ))}
        </div>
      </div>

      {/* Step Content */}
      <div className="bg-white rounded-lg shadow-sm border p-6">
        {/* Step 1: Group Selection */}
        {currentStep === 'groups' && (
          <div>
            <h2 className="text-lg font-semibold mb-4 flex items-center gap-2">
              <Users className="w-5 h-5" />
              Select Employee Groups
            </h2>
            <p className="text-gray-600 mb-4">
              Select one or more employee groups to include in the export.
              Leave all unchecked to include all groups.
            </p>

            {groupsLoading ? (
              <div className="flex items-center justify-center py-8">
                <Loader2 className="w-6 h-6 animate-spin text-blue-600" />
              </div>
            ) : (
              <div className="border rounded-lg divide-y">
                <div className="grid grid-cols-3 gap-4 px-4 py-2 bg-gray-50 font-medium text-sm text-gray-600">
                  <div>Code</div>
                  <div>Description</div>
                  <div className="text-right">Employees</div>
                </div>
                {groupsData?.groups?.map((group: EmployeeGroup) => (
                  <label
                    key={group.code}
                    className="grid grid-cols-3 gap-4 px-4 py-3 hover:bg-gray-50 cursor-pointer"
                  >
                    <div className="flex items-center gap-3">
                      <input
                        type="checkbox"
                        checked={selectedGroups.includes(group.code)}
                        onChange={(e) => {
                          if (e.target.checked) {
                            setSelectedGroups([...selectedGroups, group.code]);
                          } else {
                            setSelectedGroups(selectedGroups.filter(g => g !== group.code));
                          }
                        }}
                        className="w-4 h-4 text-blue-600 rounded"
                      />
                      <span className="font-mono">{group.code}</span>
                    </div>
                    <div>{group.description}</div>
                    <div className="text-right text-gray-600">{group.employee_count}</div>
                  </label>
                ))}
              </div>
            )}
          </div>
        )}

        {/* Step 2: Scheme & Period Selection */}
        {currentStep === 'scheme' && (
          <div className="space-y-6">
            <div>
              <h2 className="text-lg font-semibold mb-4 flex items-center gap-2">
                <Building2 className="w-5 h-5" />
                Select Pension Scheme
              </h2>

              {/* Process Type */}
              <div className="mb-6 p-4 bg-gray-50 rounded-lg">
                <div className="text-sm font-medium text-gray-700 mb-2">Process Type</div>
                <div className="flex gap-4">
                  <label className="flex items-center gap-2">
                    <input
                      type="radio"
                      name="processType"
                      checked={processType === 'report'}
                      onChange={() => setProcessType('report')}
                      className="w-4 h-4 text-blue-600"
                    />
                    <span>Report only</span>
                  </label>
                  <label className="flex items-center gap-2">
                    <input
                      type="radio"
                      name="processType"
                      checked={processType === 'file'}
                      onChange={() => setProcessType('file')}
                      className="w-4 h-4 text-blue-600"
                    />
                    <span>Create contribution file and report</span>
                  </label>
                </div>
              </div>

              {/* Scheme Selection */}
              <div className="border rounded-lg divide-y mb-6">
                <div className="grid grid-cols-4 gap-4 px-4 py-2 bg-gray-50 font-medium text-sm text-gray-600">
                  <div>Scheme Reference</div>
                  <div>Code</div>
                  <div>Description</div>
                  <div className="text-center">Selected</div>
                </div>
                {schemesLoading ? (
                  <div className="flex items-center justify-center py-8">
                    <Loader2 className="w-6 h-6 animate-spin text-blue-600" />
                  </div>
                ) : (
                  schemesData?.schemes?.map((scheme: PensionScheme) => (
                    <label
                      key={scheme.code}
                      className={`grid grid-cols-4 gap-4 px-4 py-3 cursor-pointer ${
                        selectedScheme === scheme.code ? 'bg-blue-50' : 'hover:bg-gray-50'
                      }`}
                    >
                      <div className="font-mono text-sm">{scheme.scheme_reference}</div>
                      <div className="font-mono">{scheme.code}</div>
                      <div>{scheme.description}</div>
                      <div className="flex justify-center">
                        <input
                          type="radio"
                          name="scheme"
                          checked={selectedScheme === scheme.code}
                          onChange={() => setSelectedScheme(scheme.code)}
                          className="w-4 h-4 text-blue-600"
                        />
                      </div>
                    </label>
                  ))
                )}
              </div>
            </div>

            {/* Period Selection */}
            <div className="grid grid-cols-2 gap-6">
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  Tax Year
                </label>
                <select
                  value={selectedTaxYear}
                  onChange={(e) => setSelectedTaxYear(e.target.value)}
                  className="w-full p-2 border rounded-lg"
                >
                  <option value="">Select tax year</option>
                  {periodsData?.tax_years?.map((year: string) => (
                    <option key={year} value={year}>
                      20{year.slice(0, 2)}/20{year.slice(2, 4)}
                    </option>
                  ))}
                </select>
              </div>
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  Period
                </label>
                <select
                  value={selectedPeriod}
                  onChange={(e) => setSelectedPeriod(parseInt(e.target.value))}
                  className="w-full p-2 border rounded-lg"
                >
                  <option value={0}>Select period</option>
                  {periodsData?.periods?.map((p: PayrollPeriod) => (
                    <option key={p.period} value={p.period}>
                      Period {p.period} - {formatPeriod(periodsData.tax_year, p.period)}
                      {p.pay_date && ` (${new Date(p.pay_date).toLocaleDateString('en-GB')})`}
                    </option>
                  ))}
                </select>
              </div>
            </div>

            {/* Payment Source */}
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-2 flex items-center gap-2">
                <CreditCard className="w-4 h-4" />
                Payment Source
              </label>
              <input
                type="text"
                value={paymentSource}
                onChange={(e) => setPaymentSource(e.target.value)}
                className="w-full p-2 border rounded-lg"
                placeholder="Bank Account"
              />
            </div>

            {/* Auto add employees checkbox */}
            <label className="flex items-center gap-2">
              <input
                type="checkbox"
                checked={autoAddEmployees}
                onChange={(e) => setAutoAddEmployees(e.target.checked)}
                className="w-4 h-4 text-blue-600 rounded"
              />
              <span>Automatically add employees to the list</span>
            </label>
          </div>
        )}

        {/* Step 3: Employee Selection */}
        {currentStep === 'employees' && (
          <div>
            <h2 className="text-lg font-semibold mb-4 flex items-center gap-2">
              <Users className="w-5 h-5" />
              Employees to Include
            </h2>
            <p className="text-gray-600 mb-4">
              Select the employees to include in the contribution file.
            </p>

            {contributionsLoading ? (
              <div className="flex items-center justify-center py-8">
                <Loader2 className="w-6 h-6 animate-spin text-blue-600" />
              </div>
            ) : contributionsData?.contributions?.length === 0 ? (
              <div className="text-center py-8 text-gray-500">
                No contribution records found for this period.
              </div>
            ) : (
              <>
                {/* Select all / none buttons */}
                <div className="flex gap-2 mb-4">
                  <button
                    onClick={() => setSelectedEmployees(
                      contributionsData?.contributions?.map((c: ContributionEmployee) => c.employee_ref) || []
                    )}
                    className="px-3 py-1 text-sm bg-gray-100 hover:bg-gray-200 rounded"
                  >
                    Select All
                  </button>
                  <button
                    onClick={() => setSelectedEmployees([])}
                    className="px-3 py-1 text-sm bg-gray-100 hover:bg-gray-200 rounded"
                  >
                    Select None
                  </button>
                </div>

                <div className="border rounded-lg divide-y max-h-96 overflow-y-auto">
                  <div className="grid grid-cols-5 gap-4 px-4 py-2 bg-gray-50 font-medium text-sm text-gray-600 sticky top-0">
                    <div>Ref</div>
                    <div>Group</div>
                    <div>Name</div>
                    <div>NI Number</div>
                    <div className="text-center">Include</div>
                  </div>
                  {contributionsData?.contributions?.map((emp: ContributionEmployee) => (
                    <label
                      key={emp.employee_ref}
                      className={`grid grid-cols-5 gap-4 px-4 py-2 cursor-pointer ${
                        selectedEmployees.includes(emp.employee_ref) ? 'bg-blue-50' : 'hover:bg-gray-50'
                      }`}
                    >
                      <div className="font-mono">{emp.employee_ref}</div>
                      <div>{emp.group}</div>
                      <div>
                        {emp.surname}, {emp.forename}
                        {emp.is_new_starter && (
                          <span className="ml-2 px-1.5 py-0.5 text-xs bg-green-100 text-green-700 rounded">New</span>
                        )}
                        {emp.is_leaver && (
                          <span className="ml-2 px-1.5 py-0.5 text-xs bg-red-100 text-red-700 rounded">Leaver</span>
                        )}
                      </div>
                      <div className="font-mono text-sm">{emp.ni_number}</div>
                      <div className="flex justify-center">
                        <input
                          type="checkbox"
                          checked={selectedEmployees.includes(emp.employee_ref)}
                          onChange={(e) => {
                            if (e.target.checked) {
                              setSelectedEmployees([...selectedEmployees, emp.employee_ref]);
                            } else {
                              setSelectedEmployees(selectedEmployees.filter(r => r !== emp.employee_ref));
                            }
                          }}
                          className="w-4 h-4 text-blue-600 rounded"
                        />
                      </div>
                    </label>
                  ))}
                </div>

                <div className="mt-4 text-sm text-gray-600">
                  {selectedEmployees.length} of {contributionsData?.contributions?.length} employees selected
                </div>
              </>
            )}
          </div>
        )}

        {/* Step 4: Summary */}
        {currentStep === 'summary' && (
          <div>
            <h2 className="text-lg font-semibold mb-4 flex items-center gap-2">
              <FileText className="w-5 h-5" />
              Summary
            </h2>
            <p className="text-gray-600 mb-6">
              Please verify the details below are correct. Then click Start to create the contribution file.
            </p>

            {/* Details Selected */}
            <div className="bg-gray-50 rounded-lg p-4 mb-6">
              <h3 className="font-medium text-gray-700 mb-3">Details Selected</h3>
              <div className="grid grid-cols-2 gap-4">
                <div>
                  <span className="text-gray-500">Scheme Reference:</span>
                  <span className="ml-2 font-medium">{currentScheme?.scheme_reference}</span>
                </div>
                <div>
                  <span className="text-gray-500">Payment Source:</span>
                  <span className="ml-2 font-medium">{paymentSource}</span>
                </div>
                <div>
                  <span className="text-gray-500">Provider:</span>
                  <span className="ml-2 font-medium">{currentScheme?.provider_name || currentScheme?.description}</span>
                </div>
                <div>
                  <span className="text-gray-500">Earnings Period:</span>
                  <span className="ml-2 font-medium">
                    {selectedTaxYear && formatPeriod(selectedTaxYear, selectedPeriod)}
                  </span>
                </div>
              </div>
            </div>

            {/* Employee Counts */}
            <div className="bg-gray-50 rounded-lg p-4 mb-6">
              <h3 className="font-medium text-gray-700 mb-3">Number of Employees</h3>
              {(() => {
                const summary = getSummary();
                return (
                  <div className="space-y-2">
                    <div className="flex justify-between">
                      <span>New starters this period:</span>
                      <span className="font-medium">{summary.new_starters}</span>
                    </div>
                    <div className="flex justify-between">
                      <span>Leavers this period:</span>
                      <span className="font-medium">{summary.leavers}</span>
                    </div>
                    <div className="flex justify-between">
                      <span>Current employees:</span>
                      <span className="font-medium">{summary.total_employees - summary.new_starters - summary.leavers}</span>
                    </div>
                    <div className="flex justify-between border-t pt-2 font-medium">
                      <span>Total:</span>
                      <span>{summary.total_employees}</span>
                    </div>
                  </div>
                );
              })()}
            </div>

            {/* Contribution Totals */}
            <div className="bg-gray-50 rounded-lg p-4 mb-6">
              <h3 className="font-medium text-gray-700 mb-3">Contribution Totals</h3>
              {(() => {
                const summary = getSummary();
                return (
                  <div className="space-y-2">
                    <div className="flex justify-between">
                      <span>Total Pensionable Earnings:</span>
                      <span className="font-medium">{formatCurrency(summary.total_pensionable_earnings)}</span>
                    </div>
                    <div className="flex justify-between">
                      <span>Total Employee Contributions:</span>
                      <span className="font-medium">{formatCurrency(summary.total_employee_contributions)}</span>
                    </div>
                    <div className="flex justify-between">
                      <span>Total Employer Contributions:</span>
                      <span className="font-medium">{formatCurrency(summary.total_employer_contributions)}</span>
                    </div>
                    <div className="flex justify-between border-t pt-2 font-medium">
                      <span>Total Contributions:</span>
                      <span>{formatCurrency(summary.total_employee_contributions + summary.total_employer_contributions)}</span>
                    </div>
                  </div>
                );
              })()}
            </div>

            {/* Export Result */}
            {exportResult && (
              <div className={`p-4 rounded-lg mb-6 ${
                exportResult.success ? 'bg-green-50 border border-green-200' : 'bg-red-50 border border-red-200'
              }`}>
                {exportResult.success ? (
                  <div className="flex items-center gap-2 text-green-700">
                    <CheckCircle className="w-5 h-5" />
                    <span>Export successful! File downloaded: {exportResult.filename}</span>
                  </div>
                ) : (
                  <div className="text-red-700">
                    <div className="flex items-center gap-2">
                      <XCircle className="w-5 h-5" />
                      <span>Export failed</span>
                    </div>
                    {exportResult.errors?.map((error, i) => (
                      <div key={i} className="ml-7 text-sm">{error}</div>
                    ))}
                  </div>
                )}
                {exportResult.warnings && exportResult.warnings.length > 0 && (
                  <div className="mt-2 text-amber-700">
                    {exportResult.warnings.map((warning, i) => (
                      <div key={i} className="flex items-center gap-2 text-sm">
                        <AlertTriangle className="w-4 h-4" />
                        {warning}
                      </div>
                    ))}
                  </div>
                )}
              </div>
            )}
          </div>
        )}
      </div>

      {/* Navigation Buttons */}
      <div className="flex justify-between mt-6">
        <button
          onClick={goBack}
          disabled={currentStepIndex === 0}
          className={`flex items-center gap-2 px-4 py-2 rounded-lg ${
            currentStepIndex === 0
              ? 'bg-gray-100 text-gray-400 cursor-not-allowed'
              : 'bg-gray-100 hover:bg-gray-200 text-gray-700'
          }`}
        >
          <ArrowLeft className="w-4 h-4" />
          Back
        </button>

        <div className="flex gap-2">
          <button
            onClick={() => {
              setCurrentStep('groups');
              setSelectedGroups([]);
              setSelectedScheme('');
              setSelectedEmployees([]);
              setExportResult(null);
            }}
            className="px-4 py-2 bg-gray-100 hover:bg-gray-200 text-gray-700 rounded-lg"
          >
            Cancel
          </button>

          {currentStep === 'summary' ? (
            <button
              onClick={handleExport}
              disabled={exporting || exportMutation.isPending}
              className="flex items-center gap-2 px-6 py-2 bg-blue-600 hover:bg-blue-700 text-white rounded-lg disabled:opacity-50"
            >
              {(exporting || exportMutation.isPending) ? (
                <Loader2 className="w-4 h-4 animate-spin" />
              ) : (
                <Download className="w-4 h-4" />
              )}
              {processType === 'file' ? 'Generate File' : 'Generate Report'}
            </button>
          ) : (
            <button
              onClick={goNext}
              disabled={!canProceed()}
              className={`flex items-center gap-2 px-6 py-2 rounded-lg ${
                canProceed()
                  ? 'bg-blue-600 hover:bg-blue-700 text-white'
                  : 'bg-gray-100 text-gray-400 cursor-not-allowed'
              }`}
            >
              Next
              <ArrowRight className="w-4 h-4" />
            </button>
          )}
        </div>
      </div>
    </div>
  );
}

export default PensionExport;
