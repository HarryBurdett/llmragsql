import { CreditCard } from 'lucide-react';

export function GoCardlessImport() {
  return (
    <div className="space-y-6">
      <div className="flex items-center gap-3">
        <CreditCard className="h-8 w-8 text-blue-600" />
        <h1 className="text-2xl font-bold text-gray-900">GoCardless Import</h1>
      </div>

      <div className="bg-white rounded-lg shadow p-6">
        <div className="text-center py-12">
          <CreditCard className="h-16 w-16 text-gray-300 mx-auto mb-4" />
          <h2 className="text-xl font-semibold text-gray-700 mb-2">Coming Soon</h2>
          <p className="text-gray-500">
            GoCardless import functionality will be available here.
          </p>
        </div>
      </div>
    </div>
  );
}
