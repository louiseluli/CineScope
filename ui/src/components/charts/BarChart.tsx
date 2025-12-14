import { Star } from "lucide-react";

interface BarChartProps {
  data: Array<{
    label: string;
    value: number;
    rating?: number;
    color?: string;
  }>;
  title?: string;
  maxBars?: number;
  color?: string;
  showRating?: boolean;
  horizontal?: boolean;
  height?: number;
  animated?: boolean;
}

export function BarChart({
  data,
  title,
  maxBars = 10,
  color = "var(--primary)",
  showRating = false,
  horizontal = true,
  height,
  animated = true,
}: BarChartProps) {
  const displayData = data.slice(0, maxBars);
  const maxValue = Math.max(...displayData.map(d => d.value), 1);

  if (horizontal) {
    return (
      <div className="w-full">
        {title && (
          <h3 className="text-lg font-semibold text-[var(--text)] mb-4">
            {title}
          </h3>
        )}
        <div className="space-y-3">
          {displayData.map((item, index) => (
            <div key={index} className="space-y-1">
              <div className="flex justify-between text-sm">
                <span className="text-[var(--text-muted)] truncate max-w-[60%]">
                  {item.label}
                </span>
                <div className="flex items-center gap-3">
                  {showRating && item.rating && (
                    <span className="text-yellow-400 text-xs flex items-center gap-1">
                      <Star className="w-3 h-3 fill-current" />
                      {item.rating.toFixed(1)}
                    </span>
                  )}
                  <span className="text-[var(--text)] font-medium">
                    {item.value}
                  </span>
                </div>
              </div>
              <div className="h-2 bg-[var(--bg-dark)] rounded-full overflow-hidden">
                <div
                  className={`h-full rounded-full ${
                    animated ? "transition-all duration-700 ease-out" : ""
                  }`}
                  style={{
                    width: `${(item.value / maxValue) * 100}%`,
                    backgroundColor: item.color || color,
                  }}
                />
              </div>
            </div>
          ))}
        </div>
      </div>
    );
  }

  // Vertical bar chart
  const chartHeight = height || 200;

  return (
    <div className="w-full">
      {title && (
        <h3 className="text-lg font-semibold text-[var(--text)] mb-4">
          {title}
        </h3>
      )}
      <div className="flex items-end gap-1" style={{ height: chartHeight }}>
        {displayData.map((item, index) => {
          const barHeight = (item.value / maxValue) * 100;
          return (
            <div
              key={index}
              className="flex-1 flex flex-col items-center gap-1 group"
            >
              <div className="relative flex flex-col items-center">
                {/* Value on hover */}
                <span className="text-xs text-[var(--text-muted)] opacity-0 group-hover:opacity-100 transition-opacity absolute -top-5">
                  {item.value}
                </span>
                {showRating && item.rating && (
                  <span className="text-xs text-yellow-400 absolute -top-8 opacity-0 group-hover:opacity-100 transition-opacity">
                    ★{item.rating.toFixed(1)}
                  </span>
                )}
              </div>
              <div
                className={`w-full rounded-t cursor-pointer hover:opacity-80 ${
                  animated ? "transition-all duration-500" : ""
                }`}
                style={{
                  height: `${barHeight}%`,
                  minHeight: item.value > 0 ? "4px" : "0",
                  backgroundColor: item.color || color,
                }}
                title={`${item.label}: ${item.value}${
                  item.rating ? ` (${item.rating.toFixed(1)}★)` : ""
                }`}
              />
              <span className="text-xs text-[var(--text-muted)] truncate max-w-full transform -rotate-45 origin-left mt-2 whitespace-nowrap">
                {item.label}
              </span>
            </div>
          );
        })}
      </div>
    </div>
  );
}

// Mini horizontal bar for inline stats
export function MiniBar({
  value,
  max,
  color = "var(--primary)",
  showValue = true,
}: {
  value: number;
  max: number;
  color?: string;
  showValue?: boolean;
}) {
  const percentage = Math.min((value / max) * 100, 100);

  return (
    <div className="flex items-center gap-2">
      <div className="flex-1 h-1.5 bg-[var(--bg-dark)] rounded-full overflow-hidden">
        <div
          className="h-full rounded-full transition-all duration-500"
          style={{ width: `${percentage}%`, backgroundColor: color }}
        />
      </div>
      {showValue && (
        <span className="text-xs text-[var(--text-muted)] min-w-[2rem] text-right">
          {value}
        </span>
      )}
    </div>
  );
}
