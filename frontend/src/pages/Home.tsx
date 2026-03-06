export function Home() {
  return (
    <div className="flex flex-col items-center justify-center py-20 text-center">
      <div
        className="w-14 h-14 rounded-xl flex items-center justify-center font-extrabold text-2xl text-white shadow-md mb-2"
        style={{ background: 'linear-gradient(135deg, #3b82f6, #8b5cf6)' }}
      >
        C
      </div>
      <div className="text-xl font-bold text-gray-800 mb-4">
        Crakd<span className="text-blue-500">.ai</span>
      </div>
      <p className="text-sm text-gray-400">Select an option from the menu to get started</p>
    </div>
  );
}

export default Home;
