import { Receipt } from 'lucide-react';
import { PageHeader } from '../components/ui';

export function Expenses() {
  return (
    <div className="space-y-6">
      <PageHeader
        icon={Receipt}
        title="Crakd.ai EXP"
        subtitle="Expense Management"
      />
      <div className="flex flex-col items-center justify-center py-16 text-center">
        <div className="p-6 rounded-2xl bg-orange-50 mb-6">
          <Receipt className="h-12 w-12 text-orange-400" />
        </div>
        <h2 className="text-xl font-semibold text-gray-800">Coming Soon</h2>
        <p className="text-sm text-gray-500 mt-2 max-w-md">
          Expense tracking and processing will be available in a future update.
        </p>
      </div>
    </div>
  );
}
