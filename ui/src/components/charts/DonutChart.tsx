import { useMemo } from "react";

interface DonutChartProps {
  data: Array<{ label: string; value: number; color?: string }>;
  size?: number;
  thickness?: number;
  showLegend?: boolean;
  title?: string;
  centerLabel?: string;
  centerValue?: string | number;
}

const DEFAULT_COLORS = [
  "#8b5cf6",
  "#14b8a6",
  "#f59e0b",
  "#ef4444",
  "#3b82f6",
  "#ec4899",
  "#10b981",
  "#6366f1",
  "#f97316",
  "#06b6d4",
];

export function DonutChart({
  data,
  size = 200,
  thickness = 40,
  showLegend = true,
  title,
  centerLabel,
  centerValue,
}: DonutChartProps) {
  const total = useMemo(
    () => data.reduce((sum, d) => sum + d.value, 0),
    [data]
  );

  const segments = useMemo(() => {
    let currentAngle = -90; // Start from top
    return data.map((item, index) => {
      const percentage = (item.value / total) * 100;
      const angle = (percentage / 100) * 360;
      const startAngle = currentAngle;
      currentAngle += angle;

      return {
        ...item,
        percentage,
        startAngle,
        endAngle: currentAngle,
        color: item.color || DEFAULT_COLORS[index % DEFAULT_COLORS.length],
      };
    });
  }, [data, total]);

  const radius = size / 2;
  const innerRadius = radius - thickness;

  const createArcPath = (
    startAngle: number,
    endAngle: number,
    outerR: number,
    innerR: number
  ) => {
    const startRad = (startAngle * Math.PI) / 180;
    const endRad = (endAngle * Math.PI) / 180;

    const x1 = radius + outerR * Math.cos(startRad);
    const y1 = radius + outerR * Math.sin(startRad);
    const x2 = radius + outerR * Math.cos(endRad);
    const y2 = radius + outerR * Math.sin(endRad);
    const x3 = radius + innerR * Math.cos(endRad);
    const y3 = radius + innerR * Math.sin(endRad);
    const x4 = radius + innerR * Math.cos(startRad);
    const y4 = radius + innerR * Math.sin(startRad);

    const largeArc = endAngle - startAngle > 180 ? 1 : 0;

    return `M ${x1} ${y1} A ${outerR} ${outerR} 0 ${largeArc} 1 ${x2} ${y2} L ${x3} ${y3} A ${innerR} ${innerR} 0 ${largeArc} 0 ${x4} ${y4} Z`;
  };

  return (
    <div className="flex flex-col items-center gap-4">
      {title && (
        <h3 className="text-lg font-semibold text-[var(--text)]">{title}</h3>
      )}

      <div className="relative">
        <svg width={size} height={size} className="transform -rotate-0">
          {segments.map((segment, index) => (
            <path
              key={index}
              d={createArcPath(
                segment.startAngle,
                segment.endAngle - 0.5,
                radius - 2,
                innerRadius
              )}
              fill={segment.color}
              className="transition-all duration-300 hover:opacity-80 cursor-pointer"
              style={{ filter: "drop-shadow(0 2px 4px rgba(0,0,0,0.3))" }}
            >
              <title>
                {segment.label}: {segment.value} (
                {segment.percentage.toFixed(1)}%)
              </title>
            </path>
          ))}
        </svg>

        {(centerLabel || centerValue) && (
          <div className="absolute inset-0 flex flex-col items-center justify-center">
            {centerValue && (
              <span className="text-2xl font-bold text-[var(--text)]">
                {centerValue}
              </span>
            )}
            {centerLabel && (
              <span className="text-sm text-[var(--text-muted)]">
                {centerLabel}
              </span>
            )}
          </div>
        )}
      </div>

      {showLegend && (
        <div className="flex flex-wrap justify-center gap-3 max-w-[300px]">
          {segments.slice(0, 8).map((segment, index) => (
            <div key={index} className="flex items-center gap-2">
              <div
                className="w-3 h-3 rounded-full"
                style={{ backgroundColor: segment.color }}
              />
              <span className="text-xs text-[var(--text-muted)]">
                {segment.label} ({segment.percentage.toFixed(0)}%)
              </span>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
