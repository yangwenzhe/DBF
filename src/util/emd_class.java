package util;

import entity.feature_t;
import entity.flow_t;
import entity.signature_t;

class node1_t {
    int i = 0;
    double val = 0;
    node1_t[] Next = null;
    int Next_point = 0;

}

class node2_t {
    int i, j;
    double val;
    node2_t[] NextC; /* NEXT COLUMN */
    node2_t[] NextR; /* NEXT ROW */
    int NextC_point = 0, NextR_point = 0;

}

//class flow_t {
//    int from; /* Feature number in signature 1 */
//    int to; /* Feature number in signature 2 */
//    double amount; /* Amount of flow from "from" to "to" */
//
//}
public class emd_class {
    final int MAX_SIG_SIZE = 5000;

    final int MAX_ITERATIONS = 10000;
    final double INFINITY = 1e200;
    final double EPSILON = 1e-10;
    final int DEBUG_LEVEL = 1;

    double Dist(feature_t F1, feature_t F2) {
        double dX = F1.X - F2.X, dY = F1.Y - F2.Y, dZ = 0;//F1.Z - F2.Z;
        return Math.pow(dX * dX + dY * dY + dZ * dZ, 0.5);
    }

    int _n1, _n2;
    double[][] _C;
    node2_t[] _X, _EndX, _EnterX;
    int _EndX_point = 0;
    int _X_point = 0;
    int _EnterX_point = 0;

    int[][] _IsX;
    node2_t[][] _RowsX, _ColsX;
    int[] _RowsX_content_point = new int[MAX_SIG_SIZE + 1], _ColsX_content_point = new int[MAX_SIG_SIZE + 1];
    double _maxW;
    double _maxC;
    int temp;

    public double emd(signature_t Signature1, signature_t Signature2, flow_t[] Flow) {

        int itr = 0;
        double totalCost;
        double w;
        node2_t[] XP;
        int XP_point = 0;
        flow_t[] FlowP = new flow_t[1];
        int FlowP_point = 0;
        node1_t[] U, V;
        U = new node1_t[MAX_SIG_SIZE + 1];
        V = new node1_t[MAX_SIG_SIZE + 1];
        int i;
        for (i = 0; i < (MAX_SIG_SIZE + 1); i++)
            U[i] = new node1_t();
        for (i = 0; i < (MAX_SIG_SIZE + 1); i++)
            V[i] = new node1_t();
        w = init(Signature1, Signature2);
        if (DEBUG_LEVEL > 1) {
            System.out.println("\nINITIAL SOLUTION:\n");
            if (DEBUG_LEVEL > 0)
                printSolution();
        }

        if (_n1 > 1 && _n2 > 1) {

            for (itr = 1; itr < MAX_ITERATIONS; itr++) {
                findBasicVariables(U, V);
                if (isOptimal(U, V) == 1)
                    break;

                newSol();
                if (DEBUG_LEVEL > 1) {
                    System.out.println("\nITERATION # " + itr + " \n");
                    if (DEBUG_LEVEL > 0)
                        printSolution();
                }
            }
            // if (itr == MAX_ITERATIONS)
            // System.out.println("emd: Maximum number of iterations has been reached
            // "+MAX_ITERATIONS);
        }
        totalCost = 0;
        if (Flow != null) {
            FlowP = Flow;
            FlowP_point = 0;
        }
        for (XP = _X, XP_point = _X_point; XP_point < _EndX_point; XP_point++) {
            if (XP == _EnterX && XP_point == _EnterX_point) /* _EnterX IS THE EMPTY SLOT */
                continue;
            if (XP[XP_point].i == Signature1.n || XP[XP_point].j == Signature2.n) /* DUMMY FEATURE */
                continue;
            if (XP[XP_point].val == 0) /* ZERO FLOW */
                continue;
            totalCost += (double) XP[XP_point].val * _C[XP[XP_point].i][XP[XP_point].j];

            if (Flow != null) {
                FlowP[FlowP_point].from = XP[XP_point].i;
                FlowP[FlowP_point].to = XP[XP_point].j;
                FlowP[FlowP_point].amount = XP[XP_point].val;
                FlowP_point++;
            }
        }
        //output the optimal solution
//        if (DEBUG_LEVEL > 0)
//            System.out.println("\n*** OPTIMAL SOLUTION (" + itr + " ITERATIONS): " + totalCost + " ***\n");
        return (totalCost / w);

    }

    int isOptimal(node1_t[] U, node1_t[] V) {

        double delta, deltaMin;
        int i, j, minI = 0, minJ = 0;
        deltaMin = INFINITY;
        for (i = 0; i < _n1; i++)
            for (j = 0; j < _n2; j++)
                if (_IsX[i][j] == 0) {
                    delta = _C[i][j] - U[i].val - V[j].val;
                    if (deltaMin > delta) {
                        deltaMin = delta;
                        minI = i;
                        minJ = j;
                    }
                }
        if (deltaMin == INFINITY) {
            System.out.println("emd: Unexpected error in isOptimal.");
            System.exit(0);
        }

        _EnterX[_EnterX_point].i = minI;
        _EnterX[_EnterX_point].j = minJ;
        if (deltaMin >= -EPSILON * _maxC)
            return 1;
        else
            return 0;

    }

    void newSol() {
        int i, j, k;
        double xMin;
        int steps;
        node2_t[][] Loop;
        Loop = new node2_t[2 * (2 * MAX_SIG_SIZE + 1)][2 * (2 * MAX_SIG_SIZE + 1)];
        int[] Loop_conetent_point;
        Loop_conetent_point = new int[2 * (2 * MAX_SIG_SIZE + 1)];
        node2_t[] CurX, LeaveX;
        LeaveX = new node2_t[1];
        int CurX_point = 0, LeaveX_point = 0, Loop_point = 0;
        i = _EnterX[_EnterX_point].i;
        j = _EnterX[_EnterX_point].j;
        _IsX[i][j] = 1;
        _EnterX[_EnterX_point].NextC = _RowsX[i];
        _EnterX[_EnterX_point].NextC_point = _RowsX_content_point[i];
        _EnterX[_EnterX_point].NextR = _ColsX[j];
        _EnterX[_EnterX_point].NextR_point = _ColsX_content_point[j];
        _EnterX[_EnterX_point].val = 0;
        _RowsX[i] = _EnterX;
        _RowsX_content_point[i] = _EnterX_point;
        _ColsX[j] = _EnterX;
        _ColsX_content_point[j] = _EnterX_point;
        steps = findLoop(Loop, Loop_point, Loop_conetent_point);
        xMin = INFINITY;
        for (k = 1; k < steps; k += 2) {
            if (Loop[k][Loop_conetent_point[k]].val < xMin) {
                LeaveX = Loop[k];
                LeaveX_point = Loop_conetent_point[k];
                xMin = Loop[k][Loop_conetent_point[k]].val;
            }

        }

        for (k = 0; k < steps; k += 2) {
            Loop[k][Loop_conetent_point[k]].val += xMin;
            Loop[k + 1][Loop_conetent_point[k + 1]].val -= xMin;
        }

        i = LeaveX[LeaveX_point].i;
        j = LeaveX[LeaveX_point].j;
        _IsX[i][j] = 0;

        if (_RowsX[i] == LeaveX && _RowsX_content_point[i] == LeaveX_point) {
            _RowsX[i] = LeaveX[LeaveX_point].NextC;
            _RowsX_content_point[i] = LeaveX[LeaveX_point].NextC_point;

        } else
            for (CurX = _RowsX[i], CurX_point = _RowsX_content_point[i]; CurX != null; temp = CurX[CurX_point].NextC_point, CurX = CurX[CurX_point].NextC, CurX_point = temp) {
                if (CurX[CurX_point].NextC == LeaveX && CurX[CurX_point].NextC_point == LeaveX_point) {
                    temp = CurX[CurX_point].NextC[CurX[CurX_point].NextC_point].NextC_point;
                    CurX[CurX_point].NextC = CurX[CurX_point].NextC[CurX[CurX_point].NextC_point].NextC;
                    CurX[CurX_point].NextC_point = temp;
                    break;
                }
            }
        if (_ColsX[j] == LeaveX && _ColsX_content_point[j] == LeaveX_point) {
            _ColsX[j] = LeaveX[LeaveX_point].NextR;
            _ColsX_content_point[j] = LeaveX[LeaveX_point].NextR_point;
        } else
            for (CurX = _ColsX[j], CurX_point = _ColsX_content_point[j]; CurX != null; temp = CurX[CurX_point].NextR_point, CurX = CurX[CurX_point].NextR, CurX_point = temp) {
                if (CurX[CurX_point].NextR == LeaveX && CurX[CurX_point].NextR_point == LeaveX_point) {
                    temp = CurX[CurX_point].NextR[CurX[CurX_point].NextR_point].NextR_point;

                    CurX[CurX_point].NextR = CurX[CurX_point].NextR[CurX[CurX_point].NextR_point].NextR;
                    CurX[CurX_point].NextR_point = temp;
                    break;

                }

            }
        _EnterX = LeaveX;
        _EnterX_point = LeaveX_point;

    }

    int findLoop(node2_t[][] Loop, int Loop_point, int[] Loop_conetent_point) {
        int i, steps;
        node2_t[][] CurX;
        node2_t[] NewX;
        int[] IsUsed;

        IsUsed = new int[2 * (MAX_SIG_SIZE + 1)];
        int NewX_point, CurX_point = 0;
        int[] CurX_content_point;
        for (i = 0; i < _n1 + _n2; i++)
            IsUsed[i] = 0;

        CurX = Loop;
        CurX_point = Loop_point;
        CurX_content_point = Loop_conetent_point;
        CurX[CurX_point] = _EnterX;
        CurX_content_point[CurX_point] = _EnterX_point;
        NewX = CurX[CurX_point];
        NewX_point = CurX_content_point[CurX_point];
        IsUsed[_EnterX_point - _X_point] = 1;
        steps = 1;

        do {

            if (steps % 2 == 1) {
                temp = _RowsX_content_point[NewX[NewX_point].i];
                NewX = _RowsX[NewX[NewX_point].i];
                NewX_point = temp;
                while (Boolean.valueOf(NewX != null && IsUsed[NewX_point - _X_point] == 1)) {
                    temp = NewX[NewX_point].NextC_point;
                    NewX = NewX[NewX_point].NextC;
                    NewX_point = temp;

                }
            } else {
                temp = _ColsX_content_point[NewX[NewX_point].j];
                NewX = _ColsX[NewX[NewX_point].j];
                NewX_point = temp;
                while (Boolean.valueOf(NewX != null && IsUsed[NewX_point - _X_point] == 1)
                        && Boolean.valueOf(NewX != _EnterX || NewX_point != _EnterX_point)) {

                    temp = NewX[NewX_point].NextR_point;
                    NewX = NewX[NewX_point].NextR;
                    NewX_point = temp;
                }
                if (Boolean.valueOf(NewX == _EnterX && NewX_point == _EnterX_point))
                    break;

            }
            if (NewX != null) {
                CurX_point++;
                CurX[CurX_point] = NewX;
                CurX_content_point[CurX_point] = NewX_point;
                IsUsed[NewX_point - _X_point] = 1;
                steps++;
            } else {

                do {

                    NewX = CurX[CurX_point];

                    NewX_point = CurX_content_point[CurX_point];
                    do {

                        if (steps % 2 == 1) {
                            temp = NewX[NewX_point].NextR_point;
                            NewX = NewX[NewX_point].NextR;
                            NewX_point = temp;

                        } else {
                            temp = NewX[NewX_point].NextC_point;
                            NewX = NewX[NewX_point].NextC;
                            NewX_point = temp;

                        }
                    } while (NewX != null && IsUsed[NewX_point - _X_point] == 1);
                    if (NewX == null) {
                        IsUsed[CurX_content_point[CurX_point] - _X_point] = 0;
                        CurX_point--;
                        steps--;
                    }
                } while (Boolean.valueOf(NewX == null && CurX_point >= Loop_point));
                IsUsed[CurX_content_point[CurX_point] - _X_point] = 0;
                CurX[CurX_point] = NewX;
                CurX_content_point[CurX_point] = NewX_point;
                IsUsed[NewX_point - _X_point] = 1;
            }
        } while (CurX_point >= Loop_point);

        if (CurX_point == Loop_point && CurX == Loop) {
            System.exit(1);
        }
        return steps;
    }

    void findBasicVariables(node1_t[] U, node1_t[] V) {

        int i, j, found;
        int UfoundNum, VfoundNum;
        node1_t u0Head, u1Head, v0Head, v1Head;
        node1_t[] CurU, PrevU, CurV, PrevV;
        int CurU_point = 0, PrevU_point = 0, CurV_point = 0, PrevV_point = 0;
        CurU = U;
        u0Head = new node1_t();
        v0Head = new node1_t();
        u1Head = new node1_t();
        v1Head = new node1_t();

        u0Head.Next = CurU;
        u0Head.Next_point = CurU_point;
        for (i = 0; i < _n1; i++) {
            CurU[CurU_point].i = i;
            CurU[CurU_point].Next = CurU;
            CurU[CurU_point].Next_point = CurU_point + 1;
            CurU_point++;
        }
        CurU_point--;
        CurU[CurU_point].Next = null;
        u1Head.Next = null;
        CurV = V;
        CurV_point = 0 + 1;
        if (_n2 > 1) {
            v0Head.Next = V;
            v0Head.Next_point = 0 + 1;

        } else
            v0Head.Next = null;
        for (j = 1; j < _n2; j++) {
            CurV[CurV_point].i = j;
            CurV[CurV_point].Next = CurV;
            CurV[CurV_point].Next_point = CurV_point + 1;
            CurV_point++;

        }
        CurV_point--;
        CurV[CurV_point].Next = null;
        v1Head.Next = null;
        V[0].i = 0;
        V[0].val = 0;
        v1Head.Next = V;
        v1Head.Next[v1Head.Next_point].Next = null;
        UfoundNum = VfoundNum = 0;
        while (UfoundNum < _n1 || VfoundNum < _n2) {
            found = 0;
            if (VfoundNum < _n2) {
                PrevV = new node1_t[1];
                PrevV_point = 0;
                PrevV[PrevV_point] = v1Head;

                for (CurV = v1Head.Next, CurV_point = v1Head.Next_point; CurV != null; temp = CurV[CurV_point].Next_point, CurV = CurV[CurV_point].Next, CurV_point = temp) {

                    j = CurV[CurV_point].i;
                    PrevU = new node1_t[1];
                    PrevU_point = 0;
                    PrevU[PrevU_point] = u0Head;

                    for (CurU = u0Head.Next, CurU_point = u0Head.Next_point; CurU != null; temp = CurU[CurU_point].Next_point, CurU = CurU[CurU_point].Next, CurU_point = temp) {
                        i = CurU[CurU_point].i;
                        if (_IsX[i][j] == 1) {
                            CurU[CurU_point].val = _C[i][j] - CurV[CurV_point].val;

                            PrevU[PrevU_point].Next = CurU[CurU_point].Next;
                            PrevU[PrevU_point].Next_point = CurU[CurU_point].Next_point;
                            CurU[CurU_point].Next = u1Head.Next;
                            CurU[CurU_point].Next_point = u1Head.Next_point;
                            u1Head.Next = CurU;
                            u1Head.Next_point = CurU_point;
                            CurU = PrevU;
                            CurU_point = PrevU_point;

                        } else {
                            PrevU = CurU;
                            PrevU_point = CurU_point;
                        }

                    }
                    PrevV[PrevV_point].Next = CurV[CurV_point].Next;
                    PrevV[PrevV_point].Next_point = CurV[CurV_point].Next_point;
                    VfoundNum++;
                    found = 1;

                }

            }

            if (UfoundNum < _n1) {

                PrevU = new node1_t[1];
                PrevU_point = 0;
                PrevU[PrevV_point] = u1Head;
                for (CurU = u1Head.Next, CurU_point = u1Head.Next_point; CurU != null; temp = CurU[CurU_point].Next_point, CurU = CurU[CurU_point].Next, CurU_point = temp) {
                    i = CurU[CurU_point].i;

                    PrevV = new node1_t[1];
                    PrevV_point = 0;
                    PrevV[PrevV_point] = v0Head;
                    for (CurV = v0Head.Next, CurV_point = v0Head.Next_point; CurV != null; temp = CurV[CurV_point].Next_point, CurV = CurV[CurV_point].Next, CurV_point = temp) {
                        j = CurV[CurV_point].i;
                        if (_IsX[i][j] == 1) {
                            CurV[CurV_point].val = _C[i][j] - CurU[CurU_point].val;
                            PrevV[PrevV_point].Next = CurV[CurV_point].Next;
                            PrevV[PrevV_point].Next_point = CurV[CurV_point].Next_point;
                            CurV[CurV_point].Next = v1Head.Next;
                            CurV[CurV_point].Next_point = v1Head.Next_point;
                            v1Head.Next = CurV;
                            v1Head.Next_point = CurV_point;
                            CurV = PrevV;
                            CurV_point = PrevV_point;
                        } else {
                            PrevV = CurV;
                            PrevV_point = CurV_point;
                        }

                    }
                    PrevU[PrevU_point].Next = CurU[CurU_point].Next;
                    PrevU[PrevU_point].Next_point = CurU[CurU_point].Next_point;
                    UfoundNum++;
                    found = 1;

                }
            }
            if (found == 0)
                System.exit(1);

        }

    }

    double init(signature_t Signature1, signature_t Signature2) {
        int i, j;
        double sSum, dSum, diff;
        double[] S, D;
        S = new double[MAX_SIG_SIZE + 1];
        D = new double[MAX_SIG_SIZE + 1];
        _n1 = Signature1.n;
        _n2 = Signature2.n;
        if (_n1 > MAX_SIG_SIZE || _n2 > MAX_SIG_SIZE) {
            System.out.println("emd: Signature size is limited to " + MAX_SIG_SIZE + "\n");
            System.exit(1);

        }
        _maxC = 0;
        _RowsX = new node2_t[MAX_SIG_SIZE + 1][MAX_SIG_SIZE + 1];
        _ColsX = new node2_t[MAX_SIG_SIZE + 1][MAX_SIG_SIZE + 1];
        _C = new double[MAX_SIG_SIZE + 1][MAX_SIG_SIZE + 1];//存储的distance

        _IsX = new int[MAX_SIG_SIZE + 1][MAX_SIG_SIZE + 1];
        _X = new node2_t[MAX_SIG_SIZE * 2];//MAX_SIG_SIZE * 2个节点
        for (i = 0; i < (MAX_SIG_SIZE * 2); i++)
            _X[i] = new node2_t();
        _EndX = new node2_t[1];
        for (i = 0; i < _n1; i++)

            for (j = 0; j < _n2; j++) {
                _C[i][j] = Dist(Signature1.Features[i], Signature2.Features[j]);

                if (_C[i][j] > _maxC)
                    _maxC = _C[i][j];//_maxC找到Dij中的最大值

            }
        sSum = 0.0;
        for (i = 0; i < _n1; i++) {
            S[i] = Signature1.Weights[i];
            sSum += Signature1.Weights[i];

            _RowsX[i] = null;
        }

        dSum = 0.0;
        for (j = 0; j < _n2; j++) {
            D[j] = Signature2.Weights[j];
            dSum += Signature2.Weights[j];
            _ColsX[j] = null;

        }
        diff = sSum - dSum;

        if (Boolean.valueOf(Math.abs(diff) >= EPSILON * sSum)) // Τ计翴粇畉
        {
            if (diff < 0.0) {
                for (j = 0; j < _n2; j++)
                    _C[_n1][j] = 0;
                S[_n1] = -diff;
                _RowsX[_n1] = null;
                _n1++;
            } else {
                for (i = 0; i < _n1; i++)
                    _C[i][_n2] = 0;
                D[_n2] = diff;

                _ColsX[_n2] = null;
                _n2++;
            }

        }
        for (i = 0; i < _n1; i++)
            for (j = 0; j < _n2; j++)
                _IsX[i][j] = 0;
        _EndX = _X;
        _EndX_point = _X_point;
        _maxW = sSum > dSum ? sSum : dSum;
        russel(S, D);
        _EnterX = _EndX;
        _EnterX_point = _EndX_point;
        _EndX_point++;

        return (double) (sSum > dSum ? dSum : sSum);
    }

    void russel(double[] S, double[] D) {
        int found = 0, minI = 0, minJ = 0;
        double deltaMin, oldVal, diff;
        double[][] Delta;
        Delta = new double[MAX_SIG_SIZE + 1][MAX_SIG_SIZE + 1];
        node1_t[] Ur, Vr;
        Ur = new node1_t[MAX_SIG_SIZE + 1];
        Vr = new node1_t[MAX_SIG_SIZE + 1];

        node1_t uHead, vHead;
        node1_t[] CurU, PrevU;
        PrevU = new node1_t[1];

        node1_t[] CurV, PrevV;
        PrevV = new node1_t[1];
        int CurU_point = 0, PrevU_point = 0;
        int CurV_point = 0, PrevV_point = 0;
        node1_t[] PrevUMinI, PrevVMinJ, Remember;
        Remember = new node1_t[1];
        PrevUMinI = new node1_t[1];
        PrevVMinJ = new node1_t[1];
        int PrevUMinI_point = 0, PrevVMinJ_point = 0, Remember_point = 0;
        CurU = Ur;
        uHead = new node1_t();

        vHead = new node1_t();

        uHead.Next = CurU;
        uHead.Next_point = CurU_point;
        int i;
        for (i = 0; i < (MAX_SIG_SIZE + 1); i++) {
            Ur[i] = new node1_t();
            Vr[i] = new node1_t();
        }
        for (i = 0; i < _n1; i++) {

            CurU[CurU_point].i = i;

            CurU[CurU_point].val = -INFINITY;
            CurU[CurU_point].Next = CurU;
            CurU[CurU_point].Next_point = CurU_point + 1;
            CurU_point++;

        }
        CurU_point--;
        CurU[CurU_point].Next = null;
        CurV = Vr;
        vHead.Next = CurV;
        vHead.Next_point = CurV_point;
        int j;
        for (j = 0; j < _n2; j++) {
            CurV[CurV_point].i = j;

            CurV[CurV_point].val = -INFINITY;
            CurV[CurV_point].Next = CurV;
            CurV[CurV_point].Next_point = CurV_point + 1;
            CurV_point++;

        }

        CurV_point--;
        CurV[CurV_point].Next = null;

        for (i = 0; i < _n1; i++)
            for (j = 0; j < _n2; j++) {

                double v;
                v = _C[i][j];
                if (Ur[i].val <= v) {
                    Ur[i].val = v;

                }
                if (Vr[j].val <= v) {
                    Vr[j].val = v;

                }

            }
        for (i = 0; i < _n1; i++)
            for (j = 0; j < _n2; j++) {
                Delta[i][j] = _C[i][j] - Ur[i].val - Vr[j].val;

            }
        do {
            found = 0;
            deltaMin = INFINITY;
            PrevU = new node1_t[1];

            PrevU_point = 0;
            PrevU[PrevU_point] = uHead;

            for (CurU = uHead.Next, CurU_point = uHead.Next_point; CurU != null; temp = CurU[CurU_point].Next_point, CurU = CurU[CurU_point].Next, CurU_point = temp) {

                i = CurU[CurU_point].i;
                PrevV = new node1_t[1];
                PrevV_point = 0;
                PrevV[PrevV_point] = vHead;

                for (CurV = vHead.Next, CurV_point = vHead.Next_point; CurV != null; temp = CurV[CurV_point].Next_point, CurV = CurV[CurV_point].Next, CurV_point = temp) {

                    j = CurV[CurV_point].i;
                    if (deltaMin > Delta[i][j]) {
                        deltaMin = Delta[i][j];
                        minI = i;
                        minJ = j;
                        PrevUMinI = PrevU;
                        PrevUMinI_point = PrevU_point;
                        PrevVMinJ = PrevV;
                        PrevVMinJ_point = PrevV_point;
                        found = 1;
                    }
                    PrevV = CurV;
                    PrevV_point = CurV_point;

                }
                PrevU = CurU;
                PrevU_point = CurU_point;

            }
            if (found == 0)
                break;

            Remember = PrevUMinI[PrevUMinI_point].Next;
            Remember_point = PrevUMinI[PrevUMinI_point].Next_point;

            addBasicVariable(minI, minJ, S, D, PrevUMinI, PrevUMinI_point, PrevVMinJ, PrevVMinJ_point, uHead);

            if (Remember == PrevUMinI[PrevUMinI_point].Next
                    && Remember_point == PrevUMinI[PrevUMinI_point].Next_point) {

                for (CurV = vHead.Next, CurV_point = vHead.Next_point; CurV != null; temp = CurV[CurV_point].Next_point, CurV = CurV[CurV_point].Next, CurV_point = temp) {
                    j = CurV[CurV_point].i;
                    if (CurV[CurV_point].val == _C[minI][j]) /* COLUMN j NEEDS UPDATING */ {
                        oldVal = CurV[CurV_point].val;
                        CurV[CurV_point].val = -INFINITY;

                        for (CurU = uHead.Next, CurU_point = uHead.Next_point; CurU != null; temp = CurU[CurU_point].Next_point, CurU = CurU[CurU_point].Next, CurU_point = temp) {
                            i = CurU[CurU_point].i;
                            if (CurV[CurV_point].val <= _C[i][j])
                                CurV[CurV_point].val = _C[i][j];

                        }
                        diff = oldVal - CurV[CurV_point].val;
                        if (Math.abs(diff) < EPSILON * _maxC)
                            for (CurU = uHead.Next, CurU_point = uHead.Next_point; CurU != null; temp = CurU[CurU_point].Next_point, CurU = CurU[CurU_point].Next, CurU_point = temp)
                                Delta[CurU[CurU_point].i][j] += diff;

                    }
                }

            } else {

                for (CurU = uHead.Next, CurU_point = uHead.Next_point; CurU != null; temp = CurU[CurU_point].Next_point, CurU = CurU[CurU_point].Next, CurU_point = temp) {
                    i = CurU[CurU_point].i;
                    if (CurU[CurU_point].val == _C[i][minJ]) /* ROW i NEEDS UPDATING */ {
                        oldVal = CurU[CurU_point].val;
                        CurU[CurU_point].val = -INFINITY;
                        for (CurV = vHead.Next, CurV_point = vHead.Next_point; CurV != null; temp = CurV[CurV_point].Next_point, CurV = CurV[CurV_point].Next, CurV_point = temp) {
                            j = CurV[CurV_point].i;
                            if (CurU[CurU_point].val <= _C[i][j])
                                CurU[CurU_point].val = _C[i][j];
                        }
                        diff = oldVal - CurU[CurU_point].val;
                        if (Math.abs(diff) < EPSILON * _maxC)
                            for (CurV = vHead.Next, CurV_point = vHead.Next_point; CurV != null; temp = CurV[CurV_point].Next_point, CurV = CurV[CurV_point].Next, CurV_point = temp)
                                Delta[i][CurV[CurV_point].i] += diff;

                    }

                }

            }

        } while (Boolean.valueOf(uHead.Next != null || vHead.Next != null));

    }

    void addBasicVariable(int minI, int minJ, double[] S, double[] D, node1_t[] PrevUMinI, int PrevUMinI_point,
                          node1_t[] PrevVMinJ, int PrevVMinJ_point, node1_t UHead) {
        double T;
        if (Math.abs(S[minI] - D[minJ]) <= EPSILON * _maxW) /* DEGENERATE CASE */ {
            T = S[minI];
            S[minI] = 0;
            D[minJ] -= T;
        } else if (S[minI] < D[minJ]) /* SUPPLY EXHAUSTED */ {
            T = S[minI];
            S[minI] = 0;
            D[minJ] -= T;
        } else /* DEMAND EXHAUSTED */ {
            T = D[minJ];
            D[minJ] = 0;
            S[minI] -= T;
        }
        _IsX[minI][minJ] = 1;
        _EndX[_EndX_point] = new node2_t();
        _EndX[_EndX_point].val = T;
        _EndX[_EndX_point].i = minI;
        _EndX[_EndX_point].j = minJ;
        _EndX[_EndX_point].NextC = _RowsX[minI];
        _EndX[_EndX_point].NextC_point = _RowsX_content_point[minI];
        _EndX[_EndX_point].NextR = _ColsX[minJ];
        _EndX[_EndX_point].NextR_point = _ColsX_content_point[minJ];
        _RowsX[minI] = _EndX;
        _RowsX_content_point[minI] = _EndX_point;
        _ColsX[minJ] = _EndX;
        _ColsX_content_point[minJ] = _EndX_point;
        _EndX_point++;

        if (S[minI] == 0 && UHead.Next[UHead.Next_point].Next != null) {
            temp = PrevUMinI[PrevUMinI_point].Next[PrevUMinI[PrevUMinI_point].Next_point].Next_point;

            PrevUMinI[PrevUMinI_point].Next = PrevUMinI[PrevUMinI_point].Next[PrevUMinI[PrevUMinI_point].Next_point].Next;

            PrevUMinI[PrevUMinI_point].Next_point = temp;

        } else {
            temp = PrevVMinJ[PrevVMinJ_point].Next[PrevVMinJ[PrevVMinJ_point].Next_point].Next_point;
            PrevVMinJ[PrevVMinJ_point].Next = PrevVMinJ[PrevVMinJ_point].Next[PrevVMinJ[PrevVMinJ_point].Next_point].Next;

            PrevVMinJ[PrevVMinJ_point].Next_point = temp;

        }

    }

    void printSolution() {
        node2_t[] P;
        int P_point = 0;
        double totalCost;

        totalCost = 0;
        if (DEBUG_LEVEL > 2)
            System.out.println("SIG1\tSIG2\tFLOW\tCOST\n");
        for (P = _X, P_point = _X_point; P_point < _EndX_point; P_point++) {
            if (Boolean.valueOf(Boolean.valueOf(P != _EnterX || P_point != _EnterX_point)
                    && _IsX[P[P_point].i][P[P_point].j] == 1)) {
                if (DEBUG_LEVEL > 2)
                    System.out.println(P[P_point].i + "\t" + P[P_point].j + "\t" + P[P_point].val + "\t"
                            + _C[P[P_point].i][P[P_point].j] + "\n");
                totalCost += (double) P[P_point].val * _C[P[P_point].i][P[P_point].j];
            }

        }
        System.out.println("COST = " + totalCost + "\n");
    }
}
