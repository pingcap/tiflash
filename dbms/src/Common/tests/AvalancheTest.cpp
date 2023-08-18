// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

/// Taken from SMHasher.

#include "AvalancheTest.h"

//-----------------------------------------------------------------------------

void PrintAvalancheDiagram(int x, int y, int reps, double scale, int * bins)
{
    const char * symbols = ".123456789X";

    for (int i = 0; i < y; i++)
    {
        printf("[");
        for (int j = 0; j < x; j++)
        {
            int k = (y - i) - 1;

            int bin = bins[k + (j * y)];

            double b = double(bin) / double(reps);
            b = fabs(b * 2 - 1);

            b *= scale;

            int s = static_cast<int>(floor(b * 10));

            if (s > 10)
                s = 10;
            if (s < 0)
                s = 0;

            printf("%c", symbols[s]);
        }

        printf("]\n");
    }
}

//----------------------------------------------------------------------------

double maxBias(std::vector<int> & counts, int reps)
{
    double worst = 0;

    for (int i = 0; i < static_cast<int>(counts.size()); i++)
    {
        double c = static_cast<double>(counts[i]) / static_cast<double>(reps);

        double d = fabs(c * 2 - 1);

        if (d > worst)
        {
            worst = d;
        }
    }

    return worst;
}

//-----------------------------------------------------------------------------
